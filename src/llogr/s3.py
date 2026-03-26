from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

import aioboto3
import structlog

from llogr.auth import AuthContext
from llogr.config import Settings
from llogr.metrics import S3_SAVE_ERRORS, S3_SAVE_SECONDS
from llogr.models import IngestionEvent

logger = structlog.get_logger(__name__)


async def ensure_bucket(settings: Settings) -> None:
    """Create the S3 bucket if it doesn't exist."""
    s3_cfg = settings.s3
    session = aioboto3.Session(
        aws_access_key_id=s3_cfg.access_key_id,
        aws_secret_access_key=s3_cfg.secret_access_key,
        region_name=s3_cfg.region,
    )
    try:
        async with session.client("s3", endpoint_url=s3_cfg.endpoint) as client:
            try:
                await client.head_bucket(Bucket=s3_cfg.bucket)
            except client.exceptions.ClientError:
                await client.create_bucket(Bucket=s3_cfg.bucket)
                logger.info("s3_bucket_created", bucket=s3_cfg.bucket)
    except Exception as e:
        logger.error("s3_ensure_bucket_failed", error=str(e))

S3_KEY_TS_FORMAT = "%Y%m%dT%H%M%SZ"


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
    """Return body.name from the first trace-create event, else 'unknown'."""
    for event in batch:
        if event.type == "trace-create":
            name = event.body.get("name")
            if name:
                return str(name)
    return "unknown"


def sanitize_trace_type(name: str) -> str:
    """Replace underscores with hyphens to keep S3 key parsing unambiguous."""
    return name.replace("_", "-")


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


async def save_batch_to_s3(
    batch: list[IngestionEvent],
    auth: AuthContext,
    settings: Settings,
    session_id: str = "none",
    trace_id: str | None = None,
    input_hash: str | None = None,
    trace_type: str | None = None,
) -> str:
    s3_cfg = settings.s3
    if trace_id is None:
        trace_id = extract_trace_id(batch)
    if input_hash is None:
        input_hash = extract_input_hash(batch)
    if trace_type is None:
        trace_type = sanitize_trace_type(extract_trace_type(batch))
    ts = datetime.now(timezone.utc).strftime(S3_KEY_TS_FORMAT)
    key = f"{auth.public_key}/{session_id}_{trace_id}_{trace_type}_{input_hash}_{ts}_{uuid.uuid4().hex}.jsonl"

    body = "\n".join(
        json.dumps(event.model_dump(), default=str) for event in batch
    )

    session = aioboto3.Session(
        aws_access_key_id=s3_cfg.access_key_id,
        aws_secret_access_key=s3_cfg.secret_access_key,
        region_name=s3_cfg.region,
    )
    with S3_SAVE_SECONDS.time():
        try:
            async with session.client("s3", endpoint_url=s3_cfg.endpoint) as client:
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
) -> list[dict]:
    """List batch keys (metadata only, no presigned URLs)."""
    s3_cfg = settings.s3
    prefix = f"{auth.public_key}/"

    session = aioboto3.Session(
        aws_access_key_id=s3_cfg.access_key_id,
        aws_secret_access_key=s3_cfg.secret_access_key,
        region_name=s3_cfg.region,
    )
    results: list[dict] = []
    async with session.client("s3", endpoint_url=s3_cfg.endpoint) as client:
        paginator = client.get_paginator("list_objects_v2")
        async for page in paginator.paginate(Bucket=s3_cfg.bucket, Prefix=prefix):
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
    return results


async def generate_presigned_urls(
    keys: list[str],
    auth: AuthContext,
    settings: Settings,
) -> list[dict]:
    """Generate presigned URLs for given keys, validating ownership."""
    s3_cfg = settings.s3
    prefix = f"{auth.public_key}/"
    results: list[dict] = []

    session = aioboto3.Session(
        aws_access_key_id=s3_cfg.access_key_id,
        aws_secret_access_key=s3_cfg.secret_access_key,
        region_name=s3_cfg.region,
    )
    async with session.client("s3", endpoint_url=s3_cfg.endpoint) as client:
        for key in keys:
            if not key.startswith(prefix):
                continue
            url = await client.generate_presigned_url(
                "get_object",
                Params={"Bucket": s3_cfg.bucket, "Key": key},
                ExpiresIn=3600,
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
) -> list[dict]:
    s3_cfg = settings.s3
    prefix = f"{auth.public_key}/"

    session = aioboto3.Session(
        aws_access_key_id=s3_cfg.access_key_id,
        aws_secret_access_key=s3_cfg.secret_access_key,
        region_name=s3_cfg.region,
    )
    results: list[dict] = []
    async with session.client("s3", endpoint_url=s3_cfg.endpoint) as client:
        paginator = client.get_paginator("list_objects_v2")
        async for page in paginator.paginate(Bucket=s3_cfg.bucket, Prefix=prefix):
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
                    ExpiresIn=3600,
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
    return results
