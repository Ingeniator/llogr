from opentelemetry.proto.common.v1.common_pb2 import AnyValue, ArrayValue, KeyValue
from opentelemetry.proto.trace.v1.trace_pb2 import Span

from llogr.routes.otel import (
    _is_genai_dialect,
    _span_attrs,
    _span_to_event,
)


def _kv(key: str, value) -> KeyValue:
    if isinstance(value, str):
        return KeyValue(key=key, value=AnyValue(string_value=value))
    if isinstance(value, bool):
        return KeyValue(key=key, value=AnyValue(bool_value=value))
    if isinstance(value, int):
        return KeyValue(key=key, value=AnyValue(int_value=value))
    if isinstance(value, float):
        return KeyValue(key=key, value=AnyValue(double_value=value))
    raise TypeError(value)


def _kv_array(key: str, values: list[str]) -> KeyValue:
    return KeyValue(
        key=key,
        value=AnyValue(
            array_value=ArrayValue(values=[AnyValue(string_value=v) for v in values])
        ),
    )


def _make_span(name: str, attrs: dict, *, span_id: bytes = b"\x01" * 8,
                trace_id: bytes = b"\x02" * 16, parent_span_id: bytes = b"") -> Span:
    kvs = []
    for k, v in attrs.items():
        if isinstance(v, list):
            kvs.append(_kv_array(k, v))
        else:
            kvs.append(_kv(k, v))
    return Span(
        name=name,
        span_id=span_id,
        trace_id=trace_id,
        parent_span_id=parent_span_id,
        start_time_unix_nano=1_772_582_400_000_000_000,
        end_time_unix_nano=1_772_582_401_000_000_000,
        attributes=kvs,
    )


def test_span_attrs_flattens_array_values() -> None:
    span = _make_span("generate_content gemini-2.0-flash", {
        "gen_ai.response.finish_reasons": ["stop"],
    })
    attrs = _span_attrs(span)
    assert attrs["gen_ai.response.finish_reasons"] == '["stop"]'


def test_is_genai_dialect_detects_adk_spans() -> None:
    assert _is_genai_dialect({"gen_ai.operation.name": "generate_content"})
    assert _is_genai_dialect({"gen_ai.system": "gcp.vertex.agent"})
    assert not _is_genai_dialect({"langfuse.observation.type": "generation"})


def test_genai_generation_span_maps_to_generation_create() -> None:
    """A generic GenAI-instrumented client (e.g. direct Gemini API) that does
    set `operation.name == "generate_content"`."""
    span = _make_span("generate_content gemini-2.0-flash", {
        "gen_ai.operation.name": "generate_content",
        "gen_ai.system": "gcp.vertex.agent",
        "gen_ai.request.model": "gemini-2.0-flash",
        "gen_ai.request.top_p": 0.9,
        "gen_ai.request.max_tokens": 1024,
        "gen_ai.usage.input_tokens": 100,
        "gen_ai.usage.output_tokens": 20,
        "gen_ai.response.finish_reasons": ["stop"],
        "gcp.vertex.agent.llm_request": '{"contents": []}',
        "gcp.vertex.agent.llm_response": '{"text": "hi"}',
        "gcp.vertex.agent.session_id": "sess-1",
        "gcp.vertex.agent.invocation_id": "inv-1",
        "user_id": "user-1",
    })

    event = _span_to_event(span, _span_attrs(span))

    assert event.type == "generation-create"
    assert event.body["model"] == "gemini-2.0-flash"
    assert event.body["usage"] == {"input": 100, "output": 20, "total": 120}
    assert event.body["modelParameters"] == {"top_p": 0.9, "max_tokens": 1024}
    assert event.body["finishReason"] == "stop"
    assert event.body["input"] == {"contents": []}
    assert event.body["output"] == {"text": "hi"}
    assert event.body["sessionId"] == "sess-1"
    assert event.body["userId"] == "user-1"
    assert event.body["metadata"]["provider"] == "gcp.vertex.agent"
    assert event.body["metadata"]["gcp.vertex.agent.invocation_id"] == "inv-1"


def test_adk_call_llm_span_has_no_operation_name_but_still_maps_to_generation() -> None:
    """Real ADK's `call_llm` span (google/adk/telemetry/tracing.py::trace_call_llm)
    never sets `gen_ai.operation.name` at all — only `gen_ai.system` and
    `gen_ai.request.model` are guaranteed present. `request.model` is what
    must trigger generation-create classification here."""
    span = _make_span("call_llm", {
        "gen_ai.system": "gcp.vertex.agent",
        "gen_ai.request.model": "gemini-2.0-flash",
        "gen_ai.request.top_p": 0.9,
        "gen_ai.request.max_tokens": 1024,
        "gen_ai.response.finish_reasons": ["stop"],
        "gcp.vertex.agent.llm_request": '{"contents": []}',
        "gcp.vertex.agent.llm_response": '{"text": "hi"}',
        "gcp.vertex.agent.session_id": "sess-1",
        "gcp.vertex.agent.invocation_id": "inv-1",
    })

    event = _span_to_event(span, _span_attrs(span))

    assert event.type == "generation-create"
    assert event.body["model"] == "gemini-2.0-flash"
    assert event.body["sessionId"] == "sess-1"


def test_genai_tool_span_maps_to_span_create() -> None:
    """Real ADK's execute_tool span (trace_tool_call) sets GEN_AI_TOOL_NAME,
    literal key `gen_ai.tool.name` (dotted), not `gen_ai.tool_name`."""
    span = _make_span("execute_tool", {
        "gen_ai.operation.name": "execute_tool",
        "gen_ai.tool.name": "search_docs",
        "gcp.vertex.agent.tool_call_args": '{"query": "refunds"}',
        "gcp.vertex.agent.tool_response": '{"result_count": 3}',
    })

    event = _span_to_event(span, _span_attrs(span))

    assert event.type == "span-create"
    assert event.body["name"] == "search_docs"
    assert event.body["input"] == {"query": "refunds"}
    assert event.body["output"] == {"result_count": 3}
    assert "model" not in event.body


def test_adk_invoke_agent_span_maps_agent_name_and_conversation_id() -> None:
    """Real ADK's invoke_agent span (trace_agent_invocation) sets
    GEN_AI_AGENT_NAME (`gen_ai.agent.name`) and GEN_AI_CONVERSATION_ID
    (`gen_ai.conversation.id`) — it carries no gcp.vertex.agent.* attributes
    at all, so session_id must fall back to conversation.id here."""
    span = _make_span("invoke_agent", {
        "gen_ai.operation.name": "invoke_agent",
        "gen_ai.agent.name": "billing-agent",
        "gen_ai.agent.description": "Handles billing questions",
        "gen_ai.conversation.id": "sess-2",
    })

    event = _span_to_event(span, _span_attrs(span))

    assert event.type == "span-create"
    assert event.body["name"] == "billing-agent"
    assert event.body["sessionId"] == "sess-2"


def test_genai_span_parent_id_and_ids() -> None:
    span = _make_span(
        "invoke_agent",
        {"gen_ai.operation.name": "invoke_agent"},
        span_id=b"\x03" * 8,
        trace_id=b"\x04" * 16,
        parent_span_id=b"\x05" * 8,
    )

    event = _span_to_event(span, _span_attrs(span))

    assert event.body["id"] == (b"\x03" * 8).hex()
    assert event.body["traceId"] == (b"\x04" * 16).hex()
    assert event.body["parentObservationId"] == (b"\x05" * 8).hex()


def test_langfuse_dialect_still_works() -> None:
    span = _make_span("llm-call", {
        "langfuse.observation.type": "generation",
        "langfuse.observation.model.name": "claude-sonnet-4-6",
        "user.id": "user-2",
        "session.id": "sess-2",
    })

    event = _span_to_event(span, _span_attrs(span))

    assert event.type == "generation-create"
    assert event.body["model"] == "claude-sonnet-4-6"
    assert event.body["userId"] == "user-2"
    assert event.body["sessionId"] == "sess-2"


def test_resource_service_name_overrides_span_name() -> None:
    span = _make_span("llm-call", {
        "langfuse.observation.type": "generation",
        "langfuse.observation.model.name": "claude-sonnet-4-6",
    })

    event = _span_to_event(span, _span_attrs(span), service_name="checkout-service")

    assert event.body["name"] == "checkout-service"


def test_resource_service_name_overrides_genai_agent_name() -> None:
    span = _make_span("generate_content gemini-2.0-flash", {
        "gen_ai.operation.name": "generate_content",
        "gen_ai.agent.name": "some-agent",
    })

    event = _span_to_event(span, _span_attrs(span), service_name="checkout-service")

    assert event.body["name"] == "checkout-service"


def test_no_service_name_leaves_span_name_untouched() -> None:
    span = _make_span("llm-call", {
        "langfuse.observation.type": "generation",
    })

    event = _span_to_event(span, _span_attrs(span))

    assert event.body["name"] == "llm-call"
