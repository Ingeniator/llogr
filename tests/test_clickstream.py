"""Tests for llogr clickstream fan-out (send_to_clickstream)."""
from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from llogr.auth import AuthContext
from llogr.clickstream import send_to_clickstream
from llogr.config import ClickstreamConfig, FeaturesConfig, S3Config, Settings
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


def _fake_settings(*clickstream_cfgs: ClickstreamConfig) -> Settings:
    return Settings(
        s3=S3Config(bucket="b", region="r", endpoint=None, access_key_id="a", secret_access_key="s"),
        clickstream=clickstream_cfgs,
        features=FeaturesConfig(store_backends=("clickstream",)),
    )


@pytest.mark.asyncio
async def test_ingest_only_forwards_to_endpoints_matching_agent_filter(monkeypatch):
    """Endpoints with `agents` set only receive batches from those agents; endpoints without a filter get everything."""
    from llogr import processing

    cfg_matching = ClickstreamConfig(name="matching", api_url="https://cb1.example.com/2/httpapi", agents=("research-assistant",))
    cfg_other = ClickstreamConfig(name="other", api_url="https://cb2.example.com/2/httpapi", agents=("support-bot",))
    cfg_unfiltered = ClickstreamConfig(name="unfiltered", api_url="https://cb3.example.com/2/httpapi")

    monkeypatch.setattr(processing, "get_settings", lambda: _fake_settings(cfg_matching, cfg_other, cfg_unfiltered))

    sent_to: list[str] = []

    async def fake_send(batch, auth, cfg, input_hash=""):
        sent_to.append(cfg.name)

    monkeypatch.setattr("llogr.clickstream.send_to_clickstream", fake_send)

    event = IngestionEvent(id="evt-1", timestamp="2026-05-19T10:00:00.000", type="trace-create", body={})
    failed = await processing.ingest([event], _AUTH, agent_name="research-assistant")

    assert failed == []
    assert set(sent_to) == {"matching", "unfiltered"}


@pytest.mark.asyncio
async def test_ingest_without_agent_name_only_reaches_unfiltered_endpoints(monkeypatch):
    from llogr import processing

    cfg_filtered = ClickstreamConfig(name="filtered", api_url="https://cb1.example.com/2/httpapi", agents=("research-assistant",))
    cfg_unfiltered = ClickstreamConfig(name="unfiltered", api_url="https://cb2.example.com/2/httpapi")

    monkeypatch.setattr(processing, "get_settings", lambda: _fake_settings(cfg_filtered, cfg_unfiltered))

    sent_to: list[str] = []

    async def fake_send(batch, auth, cfg, input_hash=""):
        sent_to.append(cfg.name)

    monkeypatch.setattr("llogr.clickstream.send_to_clickstream", fake_send)

    event = IngestionEvent(id="evt-1", timestamp="2026-05-19T10:00:00.000", type="trace-create", body={})
    failed = await processing.ingest([event], _AUTH)

    assert failed == []
    assert sent_to == ["unfiltered"]
