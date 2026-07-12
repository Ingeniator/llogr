"""Tests for llogr Tempo sink (transform_to_otlp / send_to_tempo)."""
from __future__ import annotations

import httpx
import pytest

from llogr.config import TempoConfig
from llogr.models import IngestionEvent
from llogr.tempo import send_to_tempo, transform_to_otlp

_TRACE_EVENT = IngestionEvent(
    id="evt-1",
    timestamp="2026-05-19T10:00:00.000Z",
    type="trace-create",
    body={"id": "trace-1", "name": "research-run"},
)

_SPAN_EVENT = IngestionEvent(
    id="evt-2",
    timestamp="2026-05-19T10:00:01.000Z",
    type="generation-create",
    body={
        "id": "gen-1",
        "traceId": "trace-1",
        "parentObservationId": "trace-1",
        "name": "call-llm",
        "startTime": "2026-05-19T10:00:01.000Z",
        "endTime": "2026-05-19T10:00:02.000Z",
        "model": "gpt-4o",
    },
)

_SCORE_EVENT = IngestionEvent(
    id="evt-3",
    timestamp="2026-05-19T10:00:02.000Z",
    type="score-create",
    body={"id": "score-1", "traceId": "trace-1", "name": "accuracy", "value": 1},
)


def test_transform_builds_spans_with_matching_trace_id():
    payload = transform_to_otlp([_TRACE_EVENT, _SPAN_EVENT], service_name="llogr")
    spans = payload["resourceSpans"][0]["scopeSpans"][0]["spans"]
    assert len(spans) == 2
    assert spans[0]["traceId"] == spans[1]["traceId"]
    assert spans[1]["parentSpanId"] == spans[0]["spanId"]


def test_transform_drops_events_with_no_span_shape():
    payload = transform_to_otlp([_SCORE_EVENT], service_name="llogr")
    assert payload["resourceSpans"][0]["scopeSpans"][0]["spans"] == []


def test_transform_sets_service_name_resource_attribute():
    payload = transform_to_otlp([_TRACE_EVENT], service_name="my-service")
    attrs = payload["resourceSpans"][0]["resource"]["attributes"]
    assert {"key": "service.name", "value": {"stringValue": "my-service"}} in attrs


@pytest.mark.asyncio
async def test_send_to_tempo_posts_otlp_payload(respx_mock):
    cfg = TempoConfig(endpoint="http://tempo:4318/v1/traces", service_name="llogr")
    route = respx_mock.post("http://tempo:4318/v1/traces").mock(return_value=httpx.Response(200))

    await send_to_tempo([_TRACE_EVENT, _SPAN_EVENT], cfg)

    assert route.called
    import json
    body = json.loads(route.calls[0].request.content)
    assert len(body["resourceSpans"][0]["scopeSpans"][0]["spans"]) == 2


@pytest.mark.asyncio
async def test_send_to_tempo_skips_request_when_batch_has_no_spans(respx_mock):
    cfg = TempoConfig(endpoint="http://tempo:4318/v1/traces", service_name="llogr")
    route = respx_mock.post("http://tempo:4318/v1/traces").mock(return_value=httpx.Response(200))

    await send_to_tempo([_SCORE_EVENT], cfg)

    assert not route.called


@pytest.mark.asyncio
async def test_send_to_tempo_raises_after_exhausting_retries(respx_mock):
    cfg = TempoConfig(endpoint="http://tempo:4318/v1/traces", service_name="llogr")
    respx_mock.post("http://tempo:4318/v1/traces").mock(side_effect=[httpx.Response(500)] * 3)

    with pytest.raises(httpx.HTTPStatusError):
        await send_to_tempo([_TRACE_EVENT], cfg)
