"""Tempo sink — converts ingested events into OTLP spans and posts them to Tempo's OTLP/HTTP receiver."""

from __future__ import annotations

import asyncio
import hashlib
from datetime import datetime, timezone

import httpx
import structlog

from llogr.config import TempoConfig
from llogr.metrics import TEMPO_FORWARD_ERRORS, TEMPO_FORWARD_SECONDS
from llogr.models import IngestionEvent

logger = structlog.get_logger(__name__)

# Event types that map onto a span; scores and other event types carry no
# duration and are dropped rather than forced into a zero-length span.
_SPAN_EVENT_TYPES = {"trace-create", "span-create", "generation-create", "event-create"}

_STATUS_CODE_ERROR = 2
_SPAN_KIND_INTERNAL = 1


def _hex_trace_id(value: str) -> str:
    """32 hex chars (16 bytes), as required by OTLP trace_id."""
    return hashlib.md5(value.encode()).hexdigest()


def _hex_span_id(value: str) -> str:
    """16 hex chars (8 bytes), as required by OTLP span_id."""
    return hashlib.sha1(value.encode()).hexdigest()[:16]


def _unix_nanos(ts: str | None) -> int:
    if not ts:
        return int(datetime.now(timezone.utc).timestamp() * 1e9)
    try:
        return int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp() * 1e9)
    except (ValueError, TypeError):
        return int(datetime.now(timezone.utc).timestamp() * 1e9)


def _attributes(body: dict) -> list[dict]:
    attrs = []
    for key in ("id", "sessionId", "userId", "model", "level", "statusMessage"):
        value = body.get(key)
        if value is not None:
            attrs.append({"key": key, "value": {"stringValue": str(value)}})
    return attrs


def transform_to_otlp(events: list[IngestionEvent], service_name: str) -> dict:
    """Build an OTLP/HTTP JSON traces payload from a batch of ingestion events."""
    spans = []
    for event in events:
        if event.type not in _SPAN_EVENT_TYPES:
            continue
        body = event.body
        trace_key = body.get("traceId") or body.get("id") or event.id
        span_key = body.get("id") or event.id
        parent_key = body.get("parentObservationId")

        start = _unix_nanos(body.get("startTime") or event.timestamp)
        end = _unix_nanos(body.get("endTime") or body.get("startTime") or event.timestamp)
        if end < start:
            end = start

        span = {
            "traceId": _hex_trace_id(str(trace_key)),
            "spanId": _hex_span_id(str(span_key)),
            "name": body.get("name") or event.type,
            "kind": _SPAN_KIND_INTERNAL,
            "startTimeUnixNano": str(start),
            "endTimeUnixNano": str(end),
            "attributes": _attributes(body),
        }
        if parent_key:
            span["parentSpanId"] = _hex_span_id(str(parent_key))
        if body.get("level") == "ERROR":
            span["status"] = {"code": _STATUS_CODE_ERROR, "message": body.get("statusMessage", "")}

        spans.append(span)

    return {
        "resourceSpans": [{
            "resource": {
                "attributes": [{"key": "service.name", "value": {"stringValue": service_name}}],
            },
            "scopeSpans": [{
                "scope": {"name": "llogr"},
                "spans": spans,
            }],
        }],
    }


async def send_to_tempo(events: list[IngestionEvent], cfg: TempoConfig) -> None:
    """POST a batch of events to Tempo's OTLP/HTTP traces endpoint."""
    payload = transform_to_otlp(events, cfg.service_name)
    spans = payload["resourceSpans"][0]["scopeSpans"][0]["spans"]
    if not spans:
        return

    max_retries = 3
    with TEMPO_FORWARD_SECONDS.time():
        for attempt in range(max_retries):
            try:
                async with httpx.AsyncClient() as client:
                    resp = await client.post(
                        cfg.endpoint,
                        json=payload,
                        headers={"Content-Type": "application/json"},
                        timeout=10,
                    )
                    resp.raise_for_status()
                break
            except Exception:
                if attempt < max_retries - 1:
                    delay = 0.5 * (2 ** attempt)
                    logger.warning("tempo_forward_retry", attempt=attempt + 1, delay=delay)
                    await asyncio.sleep(delay)
                else:
                    TEMPO_FORWARD_ERRORS.inc()
                    raise

    logger.info("forwarded_to_tempo", events=len(spans))
