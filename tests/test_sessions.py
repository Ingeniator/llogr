"""Tests for GET /api/public/sessions/{session_id} — ClickHouse path and S3 fallback."""
from __future__ import annotations

import base64
import json
from contextlib import asynccontextmanager, contextmanager
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from llogr.auth import AuthContext
from llogr.config import ClickHouseConfig, S3Config, Settings, get_settings
from llogr.main import app
from llogr.s3 import get_session_traces_s3, save_batch_to_s3
from llogr.models import IngestionEvent

BUCKET = "test-bucket"

_S3_CFG = S3Config(
    bucket=BUCKET,
    region="us-east-1",
    endpoint=None,
    access_key_id="testing",
    secret_access_key="testing",
)

MAIN_KEY = "pk-test/sess-abc_tr-1_chat_abc12345_20260101T000000Z_aabbccdd.jsonl"
POINTER_PREFIX = "pk-test/.sessions/sess-abc/"


@pytest.fixture
def auth() -> AuthContext:
    return AuthContext(public_key="pk-test", secret_key="sk-test")


@pytest.fixture
def settings_s3_only() -> Settings:
    return Settings(s3=_S3_CFG)


@pytest.fixture
def auth_headers() -> dict[str, str]:
    creds = base64.b64encode(b"pk-test:sk-test").decode()
    return {"Authorization": f"Basic {creds}"}


@contextmanager
def _override_settings(settings: Settings):
    app.dependency_overrides[get_settings] = lambda: settings
    try:
        yield
    finally:
        app.dependency_overrides.pop(get_settings, None)


def _jsonl(*events: dict) -> bytes:
    return b"\n".join(json.dumps(e).encode() for e in events)


def _make_s3_client(pointer_keys: list[str], key_bodies: dict[str, bytes]) -> MagicMock:
    """Build a mock S3 client that serves pointer listing and object content."""
    async def _get_object(Bucket, Key):  # noqa: N803
        body = AsyncMock()
        body.read = AsyncMock(return_value=key_bodies.get(Key, b""))
        return {"Body": body}

    async def _paginate(**kwargs):
        prefix = kwargs.get("Prefix", "")
        matching = [k for k in pointer_keys if k.startswith(prefix)]
        yield {"Contents": [{"Key": k} for k in matching]}

    mock_paginator = MagicMock()
    mock_paginator.paginate = _paginate

    mock_client = AsyncMock()
    mock_client.get_object = _get_object
    mock_client.get_paginator = MagicMock(return_value=mock_paginator)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    mock_session = MagicMock()
    mock_session.client.return_value = mock_client
    return mock_session


class _AsyncIter:
    """Minimal async iterator over a list of pages."""
    def __init__(self, pages):
        self._pages = pages

    def __aiter__(self):
        return self._aiter()

    async def _aiter(self):
        for page in self._pages:
            yield page

    def paginate(self, **kwargs):
        return self


GEN_EVENT = {
    "id": "evt-1",
    "timestamp": "2026-01-01T00:01:00Z",
    "type": "generation-create",
    "project_id": "pk-test",
    "model": "gpt-4o",
    "session_id": "sess-abc",
    "trace_id": "tr-1",
    "body": {"input": "hello"},
}
SPAN_EVENT = {
    "id": "evt-2",
    "timestamp": "2026-01-01T00:00:30Z",
    "type": "span-create",
    "project_id": "pk-test",
    "model": "",
    "session_id": "sess-abc",
    "trace_id": "tr-1",
    "body": {},
}
TRACE_CREATE_EVENT = {
    "id": "evt-0",
    "timestamp": "2026-01-01T00:00:00Z",
    "type": "trace-create",
    "project_id": "pk-test",
    "model": "",
    "session_id": "sess-abc",
    "trace_id": "tr-1",
    "body": {},
}


# ---------------------------------------------------------------------------
# Unit tests: get_session_traces_s3 (pointer-based)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_s3_session_traces_happy_path(auth, settings_s3_only):
    pointer_key = POINTER_PREFIX + "aabb1122"
    mock_session = _make_s3_client(
        pointer_keys=[pointer_key],
        key_bodies={
            pointer_key: MAIN_KEY.encode(),
            MAIN_KEY: _jsonl(GEN_EVENT, SPAN_EVENT),
        },
    )

    with patch("llogr.s3.aioboto3.Session", return_value=mock_session):
        traces = await get_session_traces_s3("sess-abc", auth, settings_s3_only)

    assert len(traces) == 2
    assert {t["event_id"] for t in traces} == {"evt-1", "evt-2"}


@pytest.mark.asyncio
async def test_s3_session_traces_filters_non_trace_event_types(auth, settings_s3_only):
    pointer_key = POINTER_PREFIX + "aabb1122"
    mock_session = _make_s3_client(
        pointer_keys=[pointer_key],
        key_bodies={
            pointer_key: MAIN_KEY.encode(),
            MAIN_KEY: _jsonl(GEN_EVENT, TRACE_CREATE_EVENT),
        },
    )

    with patch("llogr.s3.aioboto3.Session", return_value=mock_session):
        traces = await get_session_traces_s3("sess-abc", auth, settings_s3_only)

    assert len(traces) == 1
    assert traces[0]["event_id"] == "evt-1"


@pytest.mark.asyncio
async def test_s3_session_traces_sorted_by_timestamp(auth, settings_s3_only):
    pointer_key = POINTER_PREFIX + "aabb1122"
    mock_session = _make_s3_client(
        pointer_keys=[pointer_key],
        key_bodies={
            pointer_key: MAIN_KEY.encode(),
            MAIN_KEY: _jsonl(GEN_EVENT, SPAN_EVENT),  # GEN is later than SPAN
        },
    )

    with patch("llogr.s3.aioboto3.Session", return_value=mock_session):
        traces = await get_session_traces_s3("sess-abc", auth, settings_s3_only)

    assert traces[0]["event_id"] == "evt-2"  # span-create 00:00:30
    assert traces[1]["event_id"] == "evt-1"  # generation-create 00:01:00


@pytest.mark.asyncio
async def test_s3_session_traces_empty_when_no_pointers(auth, settings_s3_only):
    mock_session = _make_s3_client(pointer_keys=[], key_bodies={})

    with patch("llogr.s3.aioboto3.Session", return_value=mock_session):
        traces = await get_session_traces_s3("sess-abc", auth, settings_s3_only)

    assert traces == []


@pytest.mark.asyncio
async def test_s3_session_traces_org_admin_sees_sibling_project(settings_s3_only):
    """Org admin querying under tenant/project-a must also find a session ingested
    under a sibling tenant/project-b — the tight per-project prefix used for
    regular callers would miss it entirely."""
    auth = AuthContext(public_key="tenant/project-a", secret_key="", is_org_admin=True)
    sibling_pointer = "tenant/project-b/.sessions/sess-abc/aabb1122"
    sibling_main_key = "tenant/project-b/sess-abc_tr-1_chat_abc12345_20260101T000000Z_aabbccdd.jsonl"
    decoy_pointer = "tenant/project-b/.sessions/other-session/ffee5566"

    mock_session = _make_s3_client(
        pointer_keys=[sibling_pointer, decoy_pointer],
        key_bodies={
            sibling_pointer: sibling_main_key.encode(),
            sibling_main_key: _jsonl(GEN_EVENT),
        },
    )

    with patch("llogr.s3.aioboto3.Session", return_value=mock_session):
        traces = await get_session_traces_s3("sess-abc", auth, settings_s3_only, is_org_admin=True)

    assert len(traces) == 1
    assert traces[0]["event_id"] == "evt-1"


@pytest.mark.asyncio
async def test_s3_session_traces_org_admin_does_not_see_other_tenant(settings_s3_only):
    """Org admin scope is bounded to their own tenant — a session under a
    different tenant must not leak in even though it's returned by the mock."""
    auth = AuthContext(public_key="tenant-a/project-a", secret_key="", is_org_admin=True)
    other_tenant_pointer = "tenant-b/project-x/.sessions/sess-abc/aabb1122"

    mock_session = _make_s3_client(
        pointer_keys=[other_tenant_pointer],
        key_bodies={other_tenant_pointer: b"tenant-b/project-x/should-not-be-fetched.jsonl"},
    )

    with patch("llogr.s3.aioboto3.Session", return_value=mock_session):
        traces = await get_session_traces_s3("sess-abc", auth, settings_s3_only, is_org_admin=True)

    assert traces == []


@pytest.mark.asyncio
async def test_s3_session_traces_super_admin_sees_any_project(settings_s3_only):
    auth = AuthContext(public_key="tenant/project-a", secret_key="", is_super_admin=True)
    other_pointer = "other-tenant/other-project/.sessions/sess-abc/aabb1122"
    other_main_key = "other-tenant/other-project/sess-abc_tr-1_chat_abc12345_20260101T000000Z_aabbccdd.jsonl"

    mock_session = _make_s3_client(
        pointer_keys=[other_pointer],
        key_bodies={other_pointer: other_main_key.encode(), other_main_key: _jsonl(GEN_EVENT)},
    )

    with patch("llogr.s3.aioboto3.Session", return_value=mock_session):
        traces = await get_session_traces_s3("sess-abc", auth, settings_s3_only, is_super_admin=True)

    assert len(traces) == 1
    assert traces[0]["event_id"] == "evt-1"


@pytest.mark.asyncio
async def test_s3_session_traces_skips_unreadable_pointer(auth, settings_s3_only):
    good_ptr = POINTER_PREFIX + "good"
    bad_ptr = POINTER_PREFIX + "bad"
    good_key = MAIN_KEY

    async def _get_object(Bucket, Key):  # noqa: N803
        if Key == bad_ptr:
            raise Exception("access denied")
        body = AsyncMock()
        content = {good_ptr: good_key.encode(), good_key: _jsonl(GEN_EVENT)}
        body.read = AsyncMock(return_value=content.get(Key, b""))
        return {"Body": body}

    mock_paginator = MagicMock()
    mock_paginator.paginate = MagicMock(return_value=_AsyncIter([
        {"Contents": [{"Key": good_ptr}, {"Key": bad_ptr}]}
    ]))
    mock_client = AsyncMock()
    mock_client.get_object = _get_object
    mock_client.get_paginator = MagicMock(return_value=mock_paginator)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_session = MagicMock()
    mock_session.client.return_value = mock_client

    with patch("llogr.s3.aioboto3.Session", return_value=mock_session):
        traces = await get_session_traces_s3("sess-abc", auth, settings_s3_only)

    assert len(traces) == 1
    assert traces[0]["event_id"] == "evt-1"


@pytest.mark.asyncio
async def test_s3_session_traces_skips_unreadable_main_file(auth, settings_s3_only):
    good_ptr = POINTER_PREFIX + "good"
    bad_ptr = POINTER_PREFIX + "bad"
    good_key = "pk-test/sess-abc_tr-1_chat_abc12345_20260101T000100Z_aabb.jsonl"
    bad_key = "pk-test/sess-abc_tr-2_chat_abc12345_20260101T000200Z_ccdd.jsonl"

    async def _get_object(Bucket, Key):  # noqa: N803
        if Key == bad_key:
            raise Exception("not found")
        body = AsyncMock()
        content = {
            good_ptr: good_key.encode(),
            bad_ptr: bad_key.encode(),
            good_key: _jsonl(GEN_EVENT),
        }
        body.read = AsyncMock(return_value=content.get(Key, b""))
        return {"Body": body}

    mock_paginator = MagicMock()
    mock_paginator.paginate = MagicMock(return_value=_AsyncIter([
        {"Contents": [{"Key": good_ptr}, {"Key": bad_ptr}]}
    ]))
    mock_client = AsyncMock()
    mock_client.get_object = _get_object
    mock_client.get_paginator = MagicMock(return_value=mock_paginator)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_session = MagicMock()
    mock_session.client.return_value = mock_client

    with patch("llogr.s3.aioboto3.Session", return_value=mock_session):
        traces = await get_session_traces_s3("sess-abc", auth, settings_s3_only)

    assert len(traces) == 1
    assert traces[0]["event_id"] == "evt-1"


@pytest.mark.asyncio
async def test_s3_session_traces_response_shape(auth, settings_s3_only):
    pointer_key = POINTER_PREFIX + "aabb1122"
    mock_session = _make_s3_client(
        pointer_keys=[pointer_key],
        key_bodies={pointer_key: MAIN_KEY.encode(), MAIN_KEY: _jsonl(GEN_EVENT)},
    )

    with patch("llogr.s3.aioboto3.Session", return_value=mock_session):
        traces = await get_session_traces_s3("sess-abc", auth, settings_s3_only)

    assert len(traces) == 1
    t = traces[0]
    assert set(t.keys()) == {"event_id", "timestamp", "project_id", "model", "session_id", "trace_id", "body"}
    assert t["event_id"] == "evt-1"
    assert t["model"] == "gpt-4o"
    assert t["body"] == {"input": "hello"}


# ---------------------------------------------------------------------------
# Write side: save_batch_to_s3 writes a pointer for sessions
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_save_batch_writes_pointer_for_session(auth, settings_s3_only):
    put_calls: list[dict] = []

    async def _put_object(**kwargs):
        put_calls.append(kwargs)

    mock_client = AsyncMock()
    mock_client.put_object = _put_object
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_session = MagicMock()
    mock_session.client.return_value = mock_client

    batch = [IngestionEvent(id="evt-1", timestamp="2026-01-01T00:00:00Z", type="trace-create", body={})]

    with patch("llogr.s3.aioboto3.Session", return_value=mock_session):
        main_key = await save_batch_to_s3(batch, auth, settings_s3_only, session_id="sess-abc")

    assert len(put_calls) == 2
    main_call = next(c for c in put_calls if c["Key"] == main_key)
    pointer_call = next(c for c in put_calls if c["Key"] != main_key)

    assert main_call["ContentType"] == "application/x-ndjson"
    assert "/.sessions/sess-abc/" in pointer_call["Key"]
    assert pointer_call["Body"] == main_key.encode()
    assert pointer_call["ContentType"] == "text/plain"


@pytest.mark.asyncio
async def test_save_batch_no_pointer_when_no_session(auth, settings_s3_only):
    put_calls: list[dict] = []

    async def _put_object(**kwargs):
        put_calls.append(kwargs)

    mock_client = AsyncMock()
    mock_client.put_object = _put_object
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_session = MagicMock()
    mock_session.client.return_value = mock_client

    batch = [IngestionEvent(id="evt-1", timestamp="2026-01-01T00:00:00Z", type="trace-create", body={})]

    with patch("llogr.s3.aioboto3.Session", return_value=mock_session):
        await save_batch_to_s3(batch, auth, settings_s3_only)  # session_id defaults to "none"

    assert len(put_calls) == 1
    assert ".sessions" not in put_calls[0]["Key"]


@pytest.mark.asyncio
async def test_save_batch_pointer_respects_key_prefix(auth):
    settings = Settings(s3=S3Config(
        bucket=BUCKET, region="us-east-1", endpoint=None,
        access_key_id="k", secret_access_key="s", key_prefix="myprefix",
    ))
    put_calls: list[dict] = []

    async def _put_object(**kwargs):
        put_calls.append(kwargs)

    mock_client = AsyncMock()
    mock_client.put_object = _put_object
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_session = MagicMock()
    mock_session.client.return_value = mock_client

    batch = [IngestionEvent(id="evt-1", timestamp="2026-01-01T00:00:00Z", type="trace-create", body={})]

    with patch("llogr.s3.aioboto3.Session", return_value=mock_session):
        await save_batch_to_s3(batch, auth, settings, session_id="sess-xyz")

    pointer_call = next(c for c in put_calls if ".sessions" in c["Key"])
    assert pointer_call["Key"].startswith("myprefix/")


# ---------------------------------------------------------------------------
# Route integration tests
# ---------------------------------------------------------------------------

@pytest.fixture
def client():
    return TestClient(app)


def test_route_uses_clickhouse_when_configured(client, auth_headers):
    settings = Settings(s3=_S3_CFG, clickhouse=ClickHouseConfig(url="http://ch:8123"))
    traces = [{"event_id": "from-ch", "timestamp": "t", "project_id": "p",
               "model": "m", "session_id": "s", "trace_id": "t", "body": {}}]

    with _override_settings(settings):
        with patch("llogr.clickhouse.get_session_traces_ch", new=AsyncMock(return_value=traces)):
            resp = client.get("/api/public/sessions/sess-abc", headers=auth_headers)

    assert resp.status_code == 200
    assert resp.json()["traces"][0]["event_id"] == "from-ch"


def test_route_falls_back_to_s3_when_no_clickhouse(client, auth_headers):
    settings = Settings(s3=_S3_CFG)
    traces = [{"event_id": "from-s3", "timestamp": "t", "project_id": "p",
               "model": "m", "session_id": "s", "trace_id": "t", "body": {}}]

    with _override_settings(settings):
        with patch("llogr.s3.get_session_traces_s3", new=AsyncMock(return_value=traces)):
            resp = client.get("/api/public/sessions/sess-abc", headers=auth_headers)

    assert resp.status_code == 200
    assert resp.json()["traces"][0]["event_id"] == "from-s3"


def test_route_returns_503_when_no_backend_configured(client, auth_headers):
    settings = Settings(s3=S3Config(
        bucket="", region="us-east-1", endpoint=None,
        access_key_id="", secret_access_key="",
    ))

    with _override_settings(settings):
        resp = client.get("/api/public/sessions/sess-abc", headers=auth_headers)

    assert resp.status_code == 503
    assert "No trace backend configured" in resp.json()["detail"]


def test_route_response_shape(client, auth_headers):
    settings = Settings(s3=_S3_CFG)
    traces = [{"event_id": "e1", "timestamp": "2026-01-01T00:00:00Z", "project_id": "pk-test",
               "model": "gpt-4o", "session_id": "sess-abc", "trace_id": "tr-1", "body": {}}]

    with _override_settings(settings):
        with patch("llogr.s3.get_session_traces_s3", new=AsyncMock(return_value=traces)):
            resp = client.get("/api/public/sessions/sess-abc", headers=auth_headers)

    assert resp.status_code == 200
    data = resp.json()
    assert data["session_id"] == "sess-abc"
    assert isinstance(data["traces"], list)
    assert data["traces"][0]["event_id"] == "e1"


def test_route_requires_auth(client):
    resp = client.get("/api/public/sessions/sess-abc")
    assert resp.status_code in (401, 403)


# ---------------------------------------------------------------------------
# GET /api/public/traces — agent_name / session_id filtering
# ---------------------------------------------------------------------------

def test_traces_route_requires_agent_name_or_session_id(client, auth_headers):
    settings = Settings(s3=_S3_CFG, clickhouse=ClickHouseConfig(url="http://ch:8123"))
    with _override_settings(settings):
        resp = client.get("/api/public/traces", headers=auth_headers)
    assert resp.status_code == 400


def test_traces_route_requires_clickhouse(client, auth_headers):
    settings = Settings(s3=_S3_CFG)
    with _override_settings(settings):
        resp = client.get("/api/public/traces?agent_name=my-agent", headers=auth_headers)
    assert resp.status_code == 503


def test_traces_route_filters_by_agent_name(client, auth_headers):
    settings = Settings(s3=_S3_CFG, clickhouse=ClickHouseConfig(url="http://ch:8123"))
    traces = [{"event_id": "e1", "event_type": "generation-create", "timestamp": "t",
               "project_id": "pk-test", "model": "m", "name": "my-agent",
               "session_id": "s", "trace_id": "t", "body": {}}]

    with _override_settings(settings):
        with patch("llogr.clickhouse.list_traces_ch", new=AsyncMock(return_value=traces)) as mock_fn:
            resp = client.get("/api/public/traces?agent_name=my-agent", headers=auth_headers)

    assert resp.status_code == 200
    data = resp.json()
    assert data["agent_name"] == "my-agent"
    assert data["traces"][0]["event_id"] == "e1"
    assert mock_fn.call_args.kwargs["agent_name"] == "my-agent"
    assert mock_fn.call_args.kwargs["session_id"] is None


def test_traces_route_filters_by_session_id(client, auth_headers):
    settings = Settings(s3=_S3_CFG, clickhouse=ClickHouseConfig(url="http://ch:8123"))
    traces = [{"event_id": "e2", "event_type": "span-create", "timestamp": "t",
               "project_id": "pk-test", "model": "", "name": "",
               "session_id": "sess-abc", "trace_id": "t", "body": {}}]

    with _override_settings(settings):
        with patch("llogr.clickhouse.list_traces_ch", new=AsyncMock(return_value=traces)) as mock_fn:
            resp = client.get("/api/public/traces?session_id=sess-abc", headers=auth_headers)

    assert resp.status_code == 200
    assert resp.json()["session_id"] == "sess-abc"
    assert mock_fn.call_args.kwargs["session_id"] == "sess-abc"


def test_traces_route_combines_agent_name_and_session_id(client, auth_headers):
    settings = Settings(s3=_S3_CFG, clickhouse=ClickHouseConfig(url="http://ch:8123"))

    with _override_settings(settings):
        with patch("llogr.clickhouse.list_traces_ch", new=AsyncMock(return_value=[])) as mock_fn:
            resp = client.get(
                "/api/public/traces?agent_name=my-agent&session_id=sess-abc",
                headers=auth_headers,
            )

    assert resp.status_code == 200
    assert mock_fn.call_args.kwargs["agent_name"] == "my-agent"
    assert mock_fn.call_args.kwargs["session_id"] == "sess-abc"


def test_traces_route_requires_auth(client):
    resp = client.get("/api/public/traces?agent_name=my-agent")
    assert resp.status_code in (401, 403)


# ---------------------------------------------------------------------------
# GET /api/public/agents — Jaeger-style service list
# ---------------------------------------------------------------------------

def test_agents_route_requires_clickhouse(client, auth_headers):
    settings = Settings(s3=_S3_CFG)
    with _override_settings(settings):
        resp = client.get("/api/public/agents", headers=auth_headers)
    assert resp.status_code == 503


def test_agents_route_response_shape(client, auth_headers):
    settings = Settings(s3=_S3_CFG, clickhouse=ClickHouseConfig(url="http://ch:8123"))

    with _override_settings(settings):
        with patch("llogr.clickhouse.list_agent_names_ch", new=AsyncMock(return_value=["agent-a", "agent-b"])):
            resp = client.get("/api/public/agents", headers=auth_headers)

    assert resp.status_code == 200
    data = resp.json()
    assert data == {"data": ["agent-a", "agent-b"], "total": 2, "limit": 0, "offset": 0, "errors": None}


def test_agents_route_defaults_to_7_day_lookback(client, auth_headers):
    settings = Settings(s3=_S3_CFG, clickhouse=ClickHouseConfig(url="http://ch:8123"))

    with _override_settings(settings):
        with patch("llogr.clickhouse.list_agent_names_ch", new=AsyncMock(return_value=[])) as mock_fn:
            resp = client.get("/api/public/agents", headers=auth_headers)

    assert resp.status_code == 200
    kwargs = mock_fn.call_args.kwargs
    assert (kwargs["end"] - kwargs["start"]) == timedelta(days=7)


def test_agents_route_requires_auth(client):
    resp = client.get("/api/public/agents")
    assert resp.status_code in (401, 403)
