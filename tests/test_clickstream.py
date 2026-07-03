"""Tests for llogr clickstream fan-out (send_to_clickstream)."""
from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from llogr.auth import AuthContext
from llogr.clickstream import send_to_clickstream
from llogr.config import ClickstreamConfig
from llogr.models import IngestionEvent

_EVENT = IngestionEvent(
    id="evt-1",
    timestamp="2026-05-19T10:00:00.000",
    type="generation-create",
    body={"model": "gpt-4o", "input": "hello"},
)

_AUTH = AuthContext(public_key="myorg/alice", secret_key="sk-secret")


@pytest.mark.asyncio
async def test_send_to_clickstream_posts_to_configured_endpoint(respx_mock):
    cfg = ClickstreamConfig(api_url="https://cb1.example.com/2/httpapi", api_key="key-1")
    route = respx_mock.post("https://cb1.example.com/2/httpapi").mock(
        return_value=httpx.Response(200)
    )
    await send_to_clickstream([_EVENT], _AUTH, cfg)
    assert route.called
    body = json.loads(route.calls[0].request.content)
    assert body["api_key"] == "key-1"
    assert body["events"][0]["event_type"] == "generation-create"


@pytest.mark.asyncio
async def test_fanout_sends_to_every_endpoint_independently(respx_mock):
    """Given multiple clickstream endpoints, each receives its own copy of the batch."""
    cfg_a = ClickstreamConfig(name="a", api_url="https://cb1.example.com/2/httpapi", api_key="key-1")
    cfg_b = ClickstreamConfig(name="b", api_url="https://cb2.example.com/2/httpapi", api_key="key-2")
    route_a = respx_mock.post("https://cb1.example.com/2/httpapi").mock(return_value=httpx.Response(200))
    route_b = respx_mock.post("https://cb2.example.com/2/httpapi").mock(return_value=httpx.Response(200))

    await asyncio.gather(
        send_to_clickstream([_EVENT], _AUTH, cfg_a),
        send_to_clickstream([_EVENT], _AUTH, cfg_b),
    )

    assert route_a.called
    assert route_b.called
    assert json.loads(route_a.calls[0].request.content)["api_key"] == "key-1"
    assert json.loads(route_b.calls[0].request.content)["api_key"] == "key-2"


@pytest.mark.asyncio
async def test_fanout_one_endpoint_failing_does_not_block_the_other(respx_mock):
    """asyncio.gather(..., return_exceptions=True) semantics used by processing.ingest."""
    cfg_ok = ClickstreamConfig(name="ok", api_url="https://cb1.example.com/2/httpapi", api_key="key-1")
    cfg_down = ClickstreamConfig(name="down", api_url="https://cb2.example.com/2/httpapi", api_key="key-2")
    respx_mock.post("https://cb1.example.com/2/httpapi").mock(return_value=httpx.Response(200))
    respx_mock.post("https://cb2.example.com/2/httpapi").mock(
        side_effect=[httpx.Response(500)] * 3  # exhausts all retries
    )

    results = await asyncio.gather(
        send_to_clickstream([_EVENT], _AUTH, cfg_ok),
        send_to_clickstream([_EVENT], _AUTH, cfg_down),
        return_exceptions=True,
    )

    assert results[0] is None
    assert isinstance(results[1], Exception)
