"""OTLP/HTTP traces endpoint — accepts Langfuse v3 SDK spans."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

import structlog
from fastapi import APIRouter, BackgroundTasks, Depends, Header, Request
from fastapi.responses import Response

from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
    ExportTraceServiceResponse,
)

from llogr.auth import AuthContext, get_auth
from llogr.models import IngestionEvent
from llogr.processing import stage1_save_raw, stage2_forward

logger = structlog.get_logger(__name__)

router = APIRouter()

# Langfuse OTEL attribute keys
_LF = "langfuse.observation."
_LF_TRACE = "langfuse.trace."


def _ts_ns_to_iso(ns: int) -> str:
    """Convert nanosecond timestamp to ISO-8601 string."""
    return datetime.fromtimestamp(ns / 1e9, tz=timezone.utc).isoformat()


def _span_attrs(span) -> dict[str, str]:
    """Flatten protobuf KeyValue list into a plain dict."""
    out: dict[str, str] = {}
    for kv in span.attributes:
        key = kv.key
        v = kv.value
        if v.HasField("string_value"):
            out[key] = v.string_value
        elif v.HasField("int_value"):
            out[key] = str(v.int_value)
        elif v.HasField("double_value"):
            out[key] = str(v.double_value)
        elif v.HasField("bool_value"):
            out[key] = str(v.bool_value)
    return out


def _try_json(val: str | None):
    """Try to parse a JSON string, return as-is if not JSON."""
    if val is None:
        return None
    try:
        return json.loads(val)
    except (json.JSONDecodeError, TypeError):
        return val


def _collect_metadata(attrs: dict[str, str], prefix: str) -> dict | None:
    """Collect dotted metadata keys into a dict."""
    meta = {}
    full = prefix + "metadata"
    for k, v in attrs.items():
        if k == full:
            return _try_json(v)
        if k.startswith(full + "."):
            meta[k[len(full) + 1:]] = _try_json(v)
    return meta or None


def _span_to_event(span, attrs: dict[str, str]) -> IngestionEvent:
    """Convert a single OTLP span with Langfuse attributes to an IngestionEvent."""
    obs_type = attrs.get(_LF + "type", "span")
    span_id = span.span_id.hex() if span.span_id else uuid.uuid4().hex
    trace_id = span.trace_id.hex() if span.trace_id else uuid.uuid4().hex
    timestamp = _ts_ns_to_iso(span.start_time_unix_nano) if span.start_time_unix_nano else datetime.now(timezone.utc).isoformat()

    body: dict = {
        "id": span_id,
        "traceId": trace_id,
        "name": span.name or "unknown",
        "startTime": timestamp,
    }

    if span.end_time_unix_nano:
        body["endTime"] = _ts_ns_to_iso(span.end_time_unix_nano)

    # Map Langfuse-specific attributes
    body["input"] = _try_json(attrs.get(_LF + "input"))
    body["output"] = _try_json(attrs.get(_LF + "output"))
    body["metadata"] = _collect_metadata(attrs, _LF)

    if obs_type in ("generation", "generation-create"):
        body["model"] = attrs.get(_LF + "model.name")
        body["usage"] = _try_json(attrs.get(_LF + "usage_details"))
        body["costDetails"] = _try_json(attrs.get(_LF + "cost_details"))
        body["modelParameters"] = _try_json(attrs.get(_LF + "model.parameters"))
        event_type = "generation-create"
    else:
        event_type = "span-create"

    # Trace-level attributes (if present)
    trace_input = _try_json(attrs.get(_LF_TRACE + "input"))
    trace_output = _try_json(attrs.get(_LF_TRACE + "output"))
    if trace_input is not None:
        body.setdefault("input", trace_input)
    if trace_output is not None:
        body.setdefault("output", trace_output)

    body["userId"] = attrs.get("user.id")
    body["sessionId"] = attrs.get("session.id")
    body["level"] = attrs.get(_LF + "level")
    body["statusMessage"] = attrs.get(_LF + "status_message")
    body["version"] = attrs.get("langfuse.version")

    # parent span
    if span.parent_span_id and span.parent_span_id != b"":
        body["parentObservationId"] = span.parent_span_id.hex()

    # Strip None values
    body = {k: v for k, v in body.items() if v is not None}

    return IngestionEvent(
        id=span_id,
        type=event_type,
        timestamp=timestamp,
        body=body,
    )


@router.post("/api/public/otel/v1/traces")
async def otel_ingest(
    request: Request,
    background_tasks: BackgroundTasks,
    auth: AuthContext = Depends(get_auth),
    x_session_id: str = Header(default="none"),
) -> Response:
    raw = await request.body()
    req = ExportTraceServiceRequest()
    req.ParseFromString(raw)

    events: list[IngestionEvent] = []
    for resource_spans in req.resource_spans:
        for scope_spans in resource_spans.scope_spans:
            for span in scope_spans.spans:
                attrs = _span_attrs(span)
                events.append(_span_to_event(span, attrs))

    if events:
        await stage1_save_raw(events, auth, session_id=x_session_id)
        background_tasks.add_task(stage2_forward, events, auth)

    logger.info("otel_ingest", spans=len(events))

    resp = ExportTraceServiceResponse()
    return Response(
        content=resp.SerializeToString(),
        media_type="application/x-protobuf",
    )
