"""OTLP/HTTP traces endpoint — accepts Langfuse v3 SDK spans and OTel
GenAI-semantic-convention spans (e.g. Google ADK)."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

import structlog
from fastapi import APIRouter, Depends, Header, Request
from fastapi.responses import Response

from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
    ExportTraceServiceResponse,
)

from llogr.auth import AuthContext, get_auth
from llogr.models import IngestionEvent
from llogr.processing import ingest

logger = structlog.get_logger(__name__)

router = APIRouter()

# Langfuse OTEL attribute keys
_LF = "langfuse.observation."
_LF_TRACE = "langfuse.trace."

# OTel GenAI semantic-convention attribute keys (Google ADK and other
# GenAI-instrumented SDKs) — see https://opentelemetry.io/docs/specs/semconv/gen-ai/
_GENAI = "gen_ai."
_ADK = "gcp.vertex.agent."

# gen_ai.* / gcp.vertex.agent.* keys already promoted to a dedicated body
# field — excluded from body["metadata"] so they aren't duplicated.
_GENAI_PROMOTED_KEYS = {
    _GENAI + "operation_name",
    _GENAI + "system",
    _GENAI + "request.model",
    _GENAI + "request.top_p",
    _GENAI + "request.max_tokens",
    _GENAI + "response.finish_reasons",
    _GENAI + "usage.input_tokens",
    _GENAI + "usage.output_tokens",
    _GENAI + "tool_name",
    _GENAI + "agent_name",
    _ADK + "llm_request",
    _ADK + "llm_response",
    _ADK + "tool_call_args",
    _ADK + "tool_response",
    _ADK + "data",
    _ADK + "session_id",
    "user_id",
}


def _ts_ns_to_iso(ns: int) -> str:
    """Convert nanosecond timestamp to ISO-8601 string."""
    return datetime.fromtimestamp(ns / 1e9, tz=timezone.utc).isoformat()


def _flatten_scalar(v) -> str | None:
    if v.HasField("string_value"):
        return v.string_value
    if v.HasField("int_value"):
        return str(v.int_value)
    if v.HasField("double_value"):
        return str(v.double_value)
    if v.HasField("bool_value"):
        return str(v.bool_value)
    return None


def _span_attrs(span) -> dict[str, str]:
    """Flatten protobuf KeyValue list into a plain dict. Array values (e.g.
    ADK's gen_ai.response.finish_reasons) are JSON-encoded."""
    out: dict[str, str] = {}
    for kv in span.attributes:
        key = kv.key
        v = kv.value
        if v.HasField("array_value"):
            items = [_flatten_scalar(item) for item in v.array_value.values]
            out[key] = json.dumps([i for i in items if i is not None])
            continue
        scalar = _flatten_scalar(v)
        if scalar is not None:
            out[key] = scalar
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


def _is_genai_dialect(attrs: dict[str, str]) -> bool:
    """True if a span carries OTel GenAI semantic-convention attributes (e.g. Google ADK)."""
    return (_GENAI + "operation_name") in attrs or (_GENAI + "system") in attrs


def _span_to_event(span, attrs: dict[str, str]) -> IngestionEvent:
    """Convert a single OTLP span to an IngestionEvent, dispatching on attribute dialect."""
    if _is_genai_dialect(attrs):
        return _span_to_event_genai(span, attrs)
    return _span_to_event_langfuse(span, attrs)


def _span_to_event_langfuse(span, attrs: dict[str, str]) -> IngestionEvent:
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

    # Agent name travels as span metadata (not a per-request ingestion header,
    # since the OTLP exporter batches spans from many requests through one
    # shared, cached client — a header couldn't be attributed to the right
    # span). Mirrors the GenAI dialect's tool_name/agent_name → name mapping.
    agent_name = (body["metadata"] or {}).get("agent_name") if body["metadata"] else None
    if agent_name:
        body["name"] = agent_name

    if obs_type in ("generation", "generation-create"):
        body["model"] = attrs.get(_LF + "model.name")
        body["usage"] = _try_json(attrs.get(_LF + "usage_details"))
        body["costDetails"] = _try_json(attrs.get(_LF + "cost_details"))
        body["modelParameters"] = _try_json(attrs.get(_LF + "model.parameters"))
        body["promptName"] = attrs.get(_LF + "prompt.name")
        body["promptVersion"] = attrs.get(_LF + "prompt.version")
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
    body["tags"] = _try_json(attrs.get(_LF_TRACE + "tags"))

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


def _collect_genai_metadata(attrs: dict[str, str]) -> dict | None:
    """Collect gen_ai.*/gcp.vertex.agent.* attributes not already promoted to a body field."""
    meta = {}
    for k, v in attrs.items():
        if k in _GENAI_PROMOTED_KEYS:
            continue
        if k.startswith(_GENAI) or k.startswith(_ADK):
            meta[k] = _try_json(v)
    system = attrs.get(_GENAI + "system")
    if system:
        meta["provider"] = system
    return meta or None


def _span_to_event_genai(span, attrs: dict[str, str]) -> IngestionEvent:
    """Convert a single OTLP span with OTel GenAI attributes (e.g. Google ADK) to an IngestionEvent."""
    operation = attrs.get(_GENAI + "operation_name", "")
    span_id = span.span_id.hex() if span.span_id else uuid.uuid4().hex
    trace_id = span.trace_id.hex() if span.trace_id else uuid.uuid4().hex
    timestamp = _ts_ns_to_iso(span.start_time_unix_nano) if span.start_time_unix_nano else datetime.now(timezone.utc).isoformat()

    name = (
        attrs.get(_GENAI + "tool_name")
        or attrs.get(_GENAI + "agent_name")
        or span.name
        or "unknown"
    )

    body: dict = {
        "id": span_id,
        "traceId": trace_id,
        "name": name,
        "startTime": timestamp,
    }

    if span.end_time_unix_nano:
        body["endTime"] = _ts_ns_to_iso(span.end_time_unix_nano)

    # ADK carries the actual LLM/tool payloads under gcp.vertex.agent.*
    body["input"] = _try_json(
        attrs.get(_ADK + "llm_request")
        or attrs.get(_ADK + "tool_call_args")
        or attrs.get(_ADK + "data")
    )
    body["output"] = _try_json(
        attrs.get(_ADK + "llm_response")
        or attrs.get(_ADK + "tool_response")
    )
    body["metadata"] = _collect_genai_metadata(attrs)

    if operation == "generate_content":
        event_type = "generation-create"
        body["model"] = attrs.get(_GENAI + "request.model")

        input_tokens = attrs.get(_GENAI + "usage.input_tokens")
        output_tokens = attrs.get(_GENAI + "usage.output_tokens")
        if input_tokens is not None or output_tokens is not None:
            inp = int(float(input_tokens)) if input_tokens is not None else 0
            out = int(float(output_tokens)) if output_tokens is not None else 0
            body["usage"] = {"input": inp, "output": out, "total": inp + out}

        params: dict = {}
        top_p = attrs.get(_GENAI + "request.top_p")
        max_tokens = attrs.get(_GENAI + "request.max_tokens")
        if top_p is not None:
            params["top_p"] = float(top_p)
        if max_tokens is not None:
            params["max_tokens"] = int(float(max_tokens))
        if params:
            body["modelParameters"] = params

        finish_reasons = _try_json(attrs.get(_GENAI + "response.finish_reasons"))
        if isinstance(finish_reasons, list) and finish_reasons:
            body["finishReason"] = str(finish_reasons[0])
    else:
        event_type = "span-create"

    body["sessionId"] = attrs.get(_ADK + "session_id")
    body["userId"] = attrs.get("user_id")

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
    auth: AuthContext = Depends(get_auth),
    x_session_id: str = Header(default="none"),
    x_agent_name: str = Header(default=""),
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
        await ingest(events, auth, session_id=x_session_id, agent_name=x_agent_name)

    logger.info("otel_ingest", spans=len(events))

    resp = ExportTraceServiceResponse()
    return Response(
        content=resp.SerializeToString(),
        media_type="application/x-protobuf",
    )
