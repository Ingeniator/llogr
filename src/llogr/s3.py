from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

import aioboto3
import structlog
from botocore.config import Config as BotoConfig

from llogr.auth import AuthContext
from llogr.clickhouse import (
    _extract_cost,
    _extract_duration_ms,
    _extract_end_time,
    _extract_finish_reason,
    _extract_parent_span_id,
    _extract_prompt_hash,
    _extract_provider,
    _extract_result_count,
    _extract_retrieval_query,
    _extract_start_time,
    _extract_tokens,
)
from llogr.config import S3Config, Settings
from llogr.metrics import S3_SAVE_ERRORS, S3_SAVE_SECONDS
from llogr.models import IngestionEvent

logger = structlog.get_logger(__name__)


def _s3_client_config(s3_cfg: S3Config) -> BotoConfig:
    return BotoConfig(
        s3={"addressing_style": s3_cfg.addressing_style, "payload_signing_enabled": False},
        signature_version="s3v4",
        request_checksum_calculation="when_required",
        response_checksum_validation="when_required",
    )


async def ensure_bucket(settings: Settings) -> None:
    """Create the S3 bucket if it doesn't exist and configure CORS.

    Bucket creation is skipped when addressing_style=path (bucket managed externally).
    CORS is always applied when cors_origins is configured.
    """
    s3_cfg = settings.s3
    session = aioboto3.Session(
        aws_access_key_id=s3_cfg.access_key_id,
        aws_secret_access_key=s3_cfg.secret_access_key,
        region_name=s3_cfg.region,
    )

    try:
        async with session.client("s3", endpoint_url=s3_cfg.endpoint, config=_s3_client_config(s3_cfg)) as client:
            try:
                await client.head_bucket(Bucket=s3_cfg.bucket)
            except client.exceptions.ClientError as e:
                code = int(e.response["Error"].get("Code", 0))
                if code == 403:
                    logger.error("s3_bucket_access_denied", bucket=s3_cfg.bucket)
                    return
                if code == 404:
                    await client.create_bucket(Bucket=s3_cfg.bucket)
                    logger.info("s3_bucket_created", bucket=s3_cfg.bucket)
                else:
                    raise
    except Exception as e:
        logger.error("s3_ensure_bucket_failed", bucket=s3_cfg.bucket, error=str(e))

    if s3_cfg.cors_origins:
        try:
            async with session.client("s3", endpoint_url=s3_cfg.endpoint, config=_s3_client_config(s3_cfg)) as client:
                await client.put_bucket_cors(
                    Bucket=s3_cfg.bucket,
                    CORSConfiguration={
                        "CORSRules": [
                            {
                                "AllowedOrigins": list(s3_cfg.cors_origins),
                                "AllowedMethods": ["GET", "HEAD"],
                                "AllowedHeaders": ["*"],
                                "MaxAgeSeconds": 3600,
                            }
                        ]
                    },
                )
                logger.info("s3_cors_configured", bucket=s3_cfg.bucket, origins=s3_cfg.cors_origins)
        except Exception as e:
            logger.error("s3_cors_failed", bucket=s3_cfg.bucket, error=str(e))

S3_KEY_TS_FORMAT = "%Y%m%dT%H%M%SZ"


def extract_session_id(batch: list[IngestionEvent]) -> str | None:
    """Extract sessionId from event body."""
    for event in batch:
        sid = event.body.get("sessionId")
        if sid:
            return str(sid)
    return None


def extract_trace_id(batch: list[IngestionEvent]) -> str:
    """Extract trace ID from batch. Uses traceId from body, or event id for trace-create."""
    for event in batch:
        if event.type == "trace-create":
            return event.id
        trace_id = event.body.get("traceId")
        if trace_id:
            return trace_id
    return "unknown"


def extract_input_hash(batch: list[IngestionEvent]) -> str:
    """Hash body.input from the first event that has one. Returns 8-char hex digest."""
    for event in batch:
        input_val = event.body.get("input")
        if input_val is not None:
            raw = json.dumps(input_val, sort_keys=True, default=str)
            return hashlib.sha256(raw.encode()).hexdigest()[:8]
    return "noinput"


def extract_trace_type(batch: list[IngestionEvent]) -> str:
    """Return body.name from the first trace-create event, or any event with a name."""
    # Prefer trace-create events
    for event in batch:
        if event.type == "trace-create":
            name = event.body.get("name")
            if name:
                return str(name)
    # Fallback: first event with a name (covers OTEL span-create/generation-create)
    for event in batch:
        name = event.body.get("name")
        if name:
            return str(name)
    return "unknown"


def sanitize_trace_type(name: str) -> str:
    """Replace underscores with hyphens to keep S3 key parsing unambiguous."""
    return name.replace("_", "-")


def _enrich_event(event: IngestionEvent, project_id: str, input_hash: str) -> dict:
    """Return event.model_dump() extended with all promoted top-level fields.

    Mirrors the column set written to ClickHouse so DuckDB can query S3 files
    using direct field access instead of JSON path extraction.
    """
    d = event.model_dump()
    body = event.body  # already a dict; model_dump() also embeds it under "body"

    ts = datetime.fromisoformat(event.timestamp).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    inp_tok, out_tok, tot_tok = _extract_tokens(body)
    inp_cost, out_cost, tot_cost = _extract_cost(body)

    d["project_id"]      = project_id
    d["input_hash"]      = input_hash
    d["model"]           = body.get("model", "") or ""
    d["name"]            = body.get("name", "") or ""
    d["trace_id"]        = body.get("traceId", "") or ""
    d["session_id"]      = body.get("sessionId", "") or ""
    d["start_time"]      = _extract_start_time(body, ts)
    d["end_time"]        = _extract_end_time(body, ts)
    d["duration_ms"]     = _extract_duration_ms(body)
    d["provider"]        = _extract_provider(body)
    d["input_tokens"]    = inp_tok
    d["output_tokens"]   = out_tok
    d["total_tokens"]    = tot_tok
    d["cost"]            = tot_cost
    d["input_cost"]      = inp_cost
    d["output_cost"]     = out_cost
    d["finish_reason"]   = _extract_finish_reason(body)
    d["retrieval_query"] = _extract_retrieval_query(body)
    d["result_count"]    = _extract_result_count(body)
    d["parent_span_id"]  = _extract_parent_span_id(body)
    d["prompt_hash"]     = _extract_prompt_hash(body)
    return d


@dataclass
class KeyMeta:
    session_id: str
    trace_id: str
    trace_type: str
    input_hash: str
    timestamp: datetime


def parse_key_meta(key: str) -> KeyMeta | None:
    """Parse S3 key: {public_key}/{session_id}_{trace_id}_{trace_type}_{input_hash}_{ts}_{uuid}.jsonl"""
    try:
        filename = key.rsplit("/", 1)[-1]
        name = filename.removesuffix(".jsonl").removesuffix(".json")
        parts = name.split("_")
        # From right: uuid[-1], ts[-2], input_hash[-3], trace_type[-4]
        ts_str = parts[-2]
        input_hash = parts[-3]
        trace_type = parts[-4]
        # Everything before trace_type: first part is session_id, rest joined is trace_id
        prefix_parts = parts[:-4]
        session_id = prefix_parts[0]
        trace_id = "_".join(prefix_parts[1:]) if len(prefix_parts) > 1 else "unknown"
        ts = datetime.strptime(ts_str, S3_KEY_TS_FORMAT).replace(tzinfo=timezone.utc)
        return KeyMeta(
            session_id=session_id,
            trace_id=trace_id,
            trace_type=trace_type,
            input_hash=input_hash,
            timestamp=ts,
        )
    except (ValueError, IndexError):
        return None


def _list_prefix(auth: AuthContext, s3_cfg, cross_org: bool = False, superadmin_access: bool = False) -> str:
    """Build S3 prefix for listing.

    cross_org (whitelisted-agent query) or SUPER_ADMIN with superadmin_access sees all logs;
    others are scoped to their group.
    """
    if cross_org or (auth.is_super_admin and superadmin_access):
        prefix = ""
    elif "/" in auth.public_key:
        group = auth.public_key.split("/", 1)[0]
        prefix = f"{group}/"
    else:
        prefix = f"{auth.public_key}/"
    if s3_cfg.key_prefix:
        prefix = f"{s3_cfg.key_prefix.strip('/')}/{prefix}"
    return prefix


async def save_batch_to_s3(
    batch: list[IngestionEvent],
    auth: AuthContext,
    settings: Settings,
    session_id: str = "none",
    trace_id: str | None = None,
    input_hash: str | None = None,
    trace_type: str | None = None,
    request_id: str = "",
) -> str:
    s3_cfg = settings.s3
    if session_id == "none":
        session_id = extract_session_id(batch) or session_id
    if trace_id is None:
        trace_id = extract_trace_id(batch)
        if trace_id == "unknown" and request_id:
            trace_id = request_id
    if input_hash is None:
        input_hash = extract_input_hash(batch)
    if trace_type is None:
        trace_type = sanitize_trace_type(extract_trace_type(batch))
    ts = datetime.now(timezone.utc).strftime(S3_KEY_TS_FORMAT)
    key = f"{auth.public_key}/{session_id}_{trace_id}_{trace_type}_{input_hash}_{ts}_{uuid.uuid4().hex}.jsonl"
    if s3_cfg.key_prefix:
        key = f"{s3_cfg.key_prefix.strip('/')}/{key}"

    body = "\n".join(
        json.dumps(_enrich_event(event, auth.public_key, input_hash), default=str)
        for event in batch
    )

    session = aioboto3.Session(
        aws_access_key_id=s3_cfg.access_key_id,
        aws_secret_access_key=s3_cfg.secret_access_key,
        region_name=s3_cfg.region,
    )
    with S3_SAVE_SECONDS.time():
        try:
            async with session.client("s3", endpoint_url=s3_cfg.endpoint, config=_s3_client_config(s3_cfg)) as client:
                await client.put_object(
                    Bucket=s3_cfg.bucket,
                    Key=key,
                    Body=body.encode(),
                    ContentType="application/x-ndjson",
                )
        except Exception:
            S3_SAVE_ERRORS.inc()
            raise

    logger.info("saved_batch_to_s3", bucket=s3_cfg.bucket, key=key)
    return key


async def list_batch_keys(
    auth: AuthContext,
    settings: Settings,
    start: datetime | None = None,
    end: datetime | None = None,
    session_id: str | None = None,
    trace_id: str | None = None,
    input_hash: str | None = None,
    trace_type: str | None = None,
    cross_org: bool = False,
    superadmin_access: bool = False,
    limit: int | None = None,
) -> list[dict]:
    """List batch keys (metadata only, no presigned URLs)."""
    s3_cfg = settings.s3
    prefix = _list_prefix(auth, s3_cfg, cross_org=cross_org, superadmin_access=superadmin_access)

    session = aioboto3.Session(
        aws_access_key_id=s3_cfg.access_key_id,
        aws_secret_access_key=s3_cfg.secret_access_key,
        region_name=s3_cfg.region,
    )
    results: list[dict] = []
    page_size = min(limit * 3, 1000) if limit else 1000
    async with session.client("s3", endpoint_url=s3_cfg.endpoint, config=_s3_client_config(s3_cfg)) as client:
        paginator = client.get_paginator("list_objects_v2")
        async for page in paginator.paginate(
            Bucket=s3_cfg.bucket,
            Prefix=prefix,
            PaginationConfig={"PageSize": page_size},
        ):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                meta = parse_key_meta(key)
                if meta is None:
                    continue
                if start and meta.timestamp < start:
                    continue
                if end and meta.timestamp > end:
                    continue
                if session_id and meta.session_id != session_id:
                    continue
                if trace_id and meta.trace_id != trace_id:
                    continue
                if input_hash and meta.input_hash != input_hash:
                    continue
                if trace_type and meta.trace_type != trace_type:
                    continue
                results.append({
                    "key": key,
                    "session_id": meta.session_id,
                    "trace_id": meta.trace_id,
                    "trace_type": meta.trace_type,
                    "input_hash": meta.input_hash,
                    "timestamp": meta.timestamp.isoformat(),
                })
                if limit and len(results) >= limit:
                    return results
    return results


async def generate_presigned_urls(
    keys: list[str],
    auth: AuthContext,
    settings: Settings,
    superadmin_access: bool = False,
) -> list[dict]:
    """Generate presigned URLs for given keys, validating ownership."""
    s3_cfg = settings.s3
    prefix = _list_prefix(auth, s3_cfg, superadmin_access=superadmin_access)
    results: list[dict] = []

    session = aioboto3.Session(
        aws_access_key_id=s3_cfg.access_key_id,
        aws_secret_access_key=s3_cfg.secret_access_key,
        region_name=s3_cfg.region,
    )
    async with session.client("s3", endpoint_url=s3_cfg.endpoint, config=_s3_client_config(s3_cfg)) as client:
        for key in keys:
            if not key.startswith(prefix):
                continue
            url = await client.generate_presigned_url(
                "get_object",
                Params={"Bucket": s3_cfg.bucket, "Key": key},
                ExpiresIn=s3_cfg.presign_expiry,
            )
            if s3_cfg.public_endpoint and s3_cfg.endpoint:
                url = url.replace(s3_cfg.endpoint, s3_cfg.public_endpoint, 1)
            results.append({"key": key, "url": url})
    return results


async def list_batch_urls(
    auth: AuthContext,
    settings: Settings,
    start: datetime,
    end: datetime,
    session_id: str | None = None,
    trace_id: str | None = None,
    input_hash: str | None = None,
    trace_type: str | None = None,
    superadmin_access: bool = False,
    limit: int | None = None,
) -> list[dict]:
    s3_cfg = settings.s3
    prefix = _list_prefix(auth, s3_cfg, superadmin_access=superadmin_access)

    session = aioboto3.Session(
        aws_access_key_id=s3_cfg.access_key_id,
        aws_secret_access_key=s3_cfg.secret_access_key,
        region_name=s3_cfg.region,
    )
    results: list[dict] = []
    page_size = min(limit * 3, 1000) if limit else 1000
    async with session.client("s3", endpoint_url=s3_cfg.endpoint, config=_s3_client_config(s3_cfg)) as client:
        paginator = client.get_paginator("list_objects_v2")
        async for page in paginator.paginate(
            Bucket=s3_cfg.bucket,
            Prefix=prefix,
            PaginationConfig={"PageSize": page_size},
        ):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                meta = parse_key_meta(key)
                if meta is None or not (start <= meta.timestamp <= end):
                    continue
                if session_id and meta.session_id != session_id:
                    continue
                if trace_id and meta.trace_id != trace_id:
                    continue
                if input_hash and meta.input_hash != input_hash:
                    continue
                if trace_type and meta.trace_type != trace_type:
                    continue
                url = await client.generate_presigned_url(
                    "get_object",
                    Params={"Bucket": s3_cfg.bucket, "Key": key},
                    ExpiresIn=s3_cfg.presign_expiry,
                )
                if s3_cfg.public_endpoint and s3_cfg.endpoint:
                    url = url.replace(s3_cfg.endpoint, s3_cfg.public_endpoint, 1)
                results.append({
                    "key": key,
                    "session_id": meta.session_id,
                    "trace_id": meta.trace_id,
                    "trace_type": meta.trace_type,
                    "input_hash": meta.input_hash,
                    "timestamp": meta.timestamp.isoformat(),
                    "url": url,
                })
                if limit and len(results) >= limit:
                    return results
    return results
