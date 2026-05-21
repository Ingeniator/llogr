"""Tests for llogr fan-out forward_batch."""
from __future__ import annotations

import base64
import json
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from llogr.auth import AuthContext
from llogr.config import ForwardTargetConfig
from llogr.forward import forward_batch
from llogr.models import IngestionEvent


_EVENT = IngestionEvent(
    id="evt-1",
    timestamp="2026-05-19T10:00:00.000",
    type="generation-create",
    body={"model": "gpt-4o", "input": "hello"},
)

_AUTH = AuthContext(public_key="myorg/alice", secret_key="sk-secret")
_TARGET = ForwardTargetConfig(url="http://langfuse-web:3000", pass_auth=True, timeout=5)


@pytest.mark.asyncio
async def test_forward_posts_to_ingestion_endpoint(respx_mock):
    route = respx_mock.post("http://langfuse-web:3000/api/public/ingestion").mock(
        return_value=httpx.Response(200, json={"ok": True})
    )
    await forward_batch([_EVENT], _AUTH, _TARGET)
    assert route.called


@pytest.mark.asyncio
async def test_forward_sends_correct_payload(respx_mock):
    captured = {}

    def _capture(request):
        captured["body"] = json.loads(request.content)
        return httpx.Response(200)

    respx_mock.post("http://langfuse-web:3000/api/public/ingestion").mock(side_effect=_capture)
    await forward_batch([_EVENT], _AUTH, _TARGET)

    assert "batch" in captured["body"]
    assert captured["body"]["batch"][0]["id"] == "evt-1"
    assert captured["body"]["batch"][0]["type"] == "generation-create"


@pytest.mark.asyncio
async def test_forward_includes_basic_auth_header(respx_mock):
    captured_headers = {}

    def _capture(request):
        captured_headers.update(dict(request.headers))
        return httpx.Response(200)

    respx_mock.post("http://langfuse-web:3000/api/public/ingestion").mock(side_effect=_capture)
    await forward_batch([_EVENT], _AUTH, _TARGET)

    expected_creds = base64.b64encode(b"myorg/alice:sk-secret").decode()
    assert captured_headers.get("authorization") == f"Basic {expected_creds}"


@pytest.mark.asyncio
async def test_forward_omits_auth_when_pass_auth_false(respx_mock):
    captured_headers = {}

    def _capture(request):
        captured_headers.update(dict(request.headers))
        return httpx.Response(200)

    target_no_auth = ForwardTargetConfig(url="http://langfuse-web:3000", pass_auth=False, timeout=5)
    respx_mock.post("http://langfuse-web:3000/api/public/ingestion").mock(side_effect=_capture)
    await forward_batch([_EVENT], _AUTH, target_no_auth)

    assert "authorization" not in captured_headers


@pytest.mark.asyncio
async def test_forward_swallows_connection_error():
    target = ForwardTargetConfig(url="http://unreachable:9999", pass_auth=False, timeout=1)
    # Should not raise even when server is unreachable
    with patch("llogr.forward.httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post.side_effect = httpx.ConnectError("connection refused")
        mock_client_cls.return_value = mock_client

        await forward_batch([_EVENT], _AUTH, target)  # must not raise


@pytest.mark.asyncio
async def test_forward_logs_but_does_not_raise_on_5xx(respx_mock):
    respx_mock.post("http://langfuse-web:3000/api/public/ingestion").mock(
        return_value=httpx.Response(503)
    )
    # Should complete without raising
    await forward_batch([_EVENT], _AUTH, _TARGET)


@pytest.mark.asyncio
async def test_forward_uses_target_credentials_over_pass_auth(respx_mock):
    """When target has its own keypair, use those instead of the caller's auth."""
    captured_headers = {}

    def _capture(request):
        captured_headers.update(dict(request.headers))
        return httpx.Response(200)

    target_with_creds = ForwardTargetConfig(
        url="http://langfuse-web:3000",
        pass_auth=True,  # pass_auth is ignored when target keys are set
        public_key="pk-lf-test",
        secret_key="sk-lf-test",
        timeout=5,
    )
    respx_mock.post("http://langfuse-web:3000/api/public/ingestion").mock(side_effect=_capture)
    # Caller auth has empty secret_key (nginx path)
    nginx_auth = AuthContext(public_key="myorg/alice", secret_key="")
    await forward_batch([_EVENT], nginx_auth, target_with_creds)

    expected_creds = base64.b64encode(b"pk-lf-test:sk-lf-test").decode()
    assert captured_headers.get("authorization") == f"Basic {expected_creds}"
