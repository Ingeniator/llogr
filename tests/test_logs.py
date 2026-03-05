from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from llogr.auth import AuthContext
from llogr.config import ClickbeatConfig, S3Config, Settings
from llogr.main import app
from llogr.s3 import KeyMeta, list_batch_keys, list_batch_urls, parse_key_meta

BUCKET = "test-bucket"


@pytest.fixture
def settings() -> Settings:
    return Settings(
        s3=S3Config(
            bucket=BUCKET,
            region="us-east-1",
            endpoint=None,
            access_key_id="testing",
            secret_access_key="testing",
        ),
        clickbeat=ClickbeatConfig(api_url="http://x", api_key="k"),
    )


@pytest.fixture
def auth() -> AuthContext:
    return AuthContext(public_key="pk-test", secret_key="sk-test")


# --- Unit tests for parse_key_meta ---


def test_parse_key_meta_valid():
    meta = parse_key_meta("pk-test/sess1_trace1_mytype_ab12cd34_20260301T120000Z_deadbeef.jsonl")
    assert meta is not None
    assert meta.session_id == "sess1"
    assert meta.trace_id == "trace1"
    assert meta.trace_type == "mytype"
    assert meta.input_hash == "ab12cd34"
    assert meta.timestamp == datetime(2026, 3, 1, 12, 0, 0, tzinfo=timezone.utc)


def test_parse_key_meta_trace_id_with_underscores():
    meta = parse_key_meta("pk-test/sess1_trace_with_underscores_mytype_ab12cd34_20260301T120000Z_deadbeef.jsonl")
    assert meta is not None
    assert meta.session_id == "sess1"
    assert meta.trace_id == "trace_with_underscores"
    assert meta.trace_type == "mytype"
    assert meta.input_hash == "ab12cd34"


def test_parse_key_meta_invalid():
    assert parse_key_meta("pk-test/bad_key.jsonl") is None
    assert parse_key_meta("") is None


# --- Unit tests for list_batch_urls ---


def _make_mock_session(keys: list[dict]):
    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.generate_presigned_url = AsyncMock(return_value="https://presigned-url")

    mock_paginator = MagicMock()

    async def async_pages(**kwargs):
        for page in [{"Contents": keys}]:
            yield page

    mock_paginator.paginate = async_pages
    mock_client.get_paginator = MagicMock(return_value=mock_paginator)

    mock_session = MagicMock()
    mock_session.client.return_value = mock_client
    return mock_session


SAMPLE_KEYS = [
    {"Key": "pk-test/sessA_tr1_default_aabb0011_20260301T100000Z_aaa.jsonl"},
    {"Key": "pk-test/sessA_tr2_chatbot_aabb0011_20260301T120000Z_bbb.jsonl"},
    {"Key": "pk-test/sessB_tr1_qa-bot_ccdd2233_20260301T140000Z_ccc.jsonl"},
]


@pytest.mark.asyncio
async def test_list_batch_urls_filters_by_timestamp(settings: Settings, auth: AuthContext):
    mock_session = _make_mock_session(SAMPLE_KEYS)
    start = datetime(2026, 3, 1, 11, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 3, 1, 13, 0, 0, tzinfo=timezone.utc)

    with patch("llogr.s3.aioboto3.Session", return_value=mock_session):
        results = await list_batch_urls(auth, settings, start, end)

    assert len(results) == 1
    assert results[0]["key"] == "pk-test/sessA_tr2_chatbot_aabb0011_20260301T120000Z_bbb.jsonl"
    assert results[0]["session_id"] == "sessA"
    assert results[0]["trace_id"] == "tr2"
    assert results[0]["trace_type"] == "chatbot"
    assert results[0]["input_hash"] == "aabb0011"


@pytest.mark.asyncio
async def test_list_batch_urls_filter_by_session_id(settings: Settings, auth: AuthContext):
    mock_session = _make_mock_session(SAMPLE_KEYS)
    start = datetime(2026, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 3, 2, 0, 0, 0, tzinfo=timezone.utc)

    with patch("llogr.s3.aioboto3.Session", return_value=mock_session):
        results = await list_batch_urls(auth, settings, start, end, session_id="sessB")

    assert len(results) == 1
    assert results[0]["session_id"] == "sessB"


@pytest.mark.asyncio
async def test_list_batch_urls_filter_by_trace_id(settings: Settings, auth: AuthContext):
    mock_session = _make_mock_session(SAMPLE_KEYS)
    start = datetime(2026, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 3, 2, 0, 0, 0, tzinfo=timezone.utc)

    with patch("llogr.s3.aioboto3.Session", return_value=mock_session):
        results = await list_batch_urls(auth, settings, start, end, trace_id="tr1")

    assert len(results) == 2
    assert all(r["trace_id"] == "tr1" for r in results)


@pytest.mark.asyncio
async def test_list_batch_urls_filter_by_input_hash(settings: Settings, auth: AuthContext):
    mock_session = _make_mock_session(SAMPLE_KEYS)
    start = datetime(2026, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 3, 2, 0, 0, 0, tzinfo=timezone.utc)

    with patch("llogr.s3.aioboto3.Session", return_value=mock_session):
        results = await list_batch_urls(auth, settings, start, end, input_hash="ccdd2233")

    assert len(results) == 1
    assert results[0]["input_hash"] == "ccdd2233"


@pytest.mark.asyncio
async def test_list_batch_urls_inclusive_boundaries(settings: Settings, auth: AuthContext):
    mock_session = _make_mock_session(SAMPLE_KEYS)
    start = datetime(2026, 3, 1, 10, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 3, 1, 14, 0, 0, tzinfo=timezone.utc)

    with patch("llogr.s3.aioboto3.Session", return_value=mock_session):
        results = await list_batch_urls(auth, settings, start, end)

    assert len(results) == 3


# --- Route tests ---


@pytest.fixture
def log_client():
    with patch("llogr.routes.ingestion.stage1_save_raw", new_callable=AsyncMock), \
         patch("llogr.routes.ingestion.stage2_forward_to_clickbeat", new_callable=AsyncMock):
        yield TestClient(app)


@pytest.fixture
def auth_headers() -> dict[str, str]:
    import base64
    creds = base64.b64encode(b"pk-test:sk-test").decode()
    return {"Authorization": f"Basic {creds}"}


def test_get_logs_returns_files(log_client: TestClient, auth_headers: dict):
    mock_files = [
        {
            "key": "pk-test/s_t_tt_h_20260301T120000Z_bbb.jsonl",
            "session_id": "s", "trace_id": "t", "trace_type": "tt", "input_hash": "h",
            "timestamp": "2026-03-01T12:00:00+00:00", "url": "https://url",
        },
    ]
    with patch("llogr.routes.logs.list_batch_urls", new_callable=AsyncMock, return_value=mock_files):
        resp = log_client.get(
            "/api/public/logs?start=2026-03-01T00:00:00Z&end=2026-03-02T00:00:00Z",
            headers=auth_headers,
        )
    assert resp.status_code == 200
    assert resp.json()["files"] == mock_files


def test_get_logs_passes_filters(log_client: TestClient, auth_headers: dict):
    with patch("llogr.routes.logs.list_batch_urls", new_callable=AsyncMock, return_value=[]) as mock_list:
        log_client.get(
            "/api/public/logs?start=2026-03-01T00:00:00Z&end=2026-03-02T00:00:00Z"
            "&session_id=sess1&trace_id=tr1&input_hash=aabb0011&trace_type=chatbot",
            headers=auth_headers,
        )
    _, kwargs = mock_list.call_args
    assert kwargs["session_id"] == "sess1"
    assert kwargs["trace_id"] == "tr1"
    assert kwargs["input_hash"] == "aabb0011"
    assert kwargs["trace_type"] == "chatbot"


def test_get_logs_requires_auth(log_client: TestClient):
    with patch("llogr.routes.logs.list_batch_urls", new_callable=AsyncMock, return_value=[]):
        resp = log_client.get("/api/public/logs?start=2026-03-01T00:00:00Z&end=2026-03-02T00:00:00Z")
    assert resp.status_code == 422


def test_get_logs_requires_params(log_client: TestClient, auth_headers: dict):
    resp = log_client.get("/api/public/logs", headers=auth_headers)
    assert resp.status_code == 422


# --- Unit tests for list_batch_keys ---


@pytest.mark.asyncio
async def test_list_batch_keys_returns_metadata(settings: Settings, auth: AuthContext):
    mock_session = _make_mock_session(SAMPLE_KEYS)
    with patch("llogr.s3.aioboto3.Session", return_value=mock_session):
        results = await list_batch_keys(auth, settings)

    assert len(results) == 3
    assert all("url" not in r for r in results)
    assert results[0]["session_id"] == "sessA"
    assert results[0]["key"] == SAMPLE_KEYS[0]["Key"]


@pytest.mark.asyncio
async def test_list_batch_keys_filters(settings: Settings, auth: AuthContext):
    mock_session = _make_mock_session(SAMPLE_KEYS)
    with patch("llogr.s3.aioboto3.Session", return_value=mock_session):
        results = await list_batch_keys(auth, settings, session_id="sessB")
    assert len(results) == 1
    assert results[0]["session_id"] == "sessB"

    mock_session = _make_mock_session(SAMPLE_KEYS)
    with patch("llogr.s3.aioboto3.Session", return_value=mock_session):
        results = await list_batch_keys(auth, settings, trace_id="tr1")
    assert len(results) == 2

    mock_session = _make_mock_session(SAMPLE_KEYS)
    with patch("llogr.s3.aioboto3.Session", return_value=mock_session):
        results = await list_batch_keys(auth, settings, input_hash="ccdd2233")
    assert len(results) == 1


# --- Route tests for new endpoints ---


def test_list_logs_endpoint(log_client: TestClient, auth_headers: dict):
    mock_files = [
        {"key": "pk-test/s_t_tt_h_20260301T120000Z_bbb.jsonl",
         "session_id": "s", "trace_id": "t", "trace_type": "tt", "input_hash": "h",
         "timestamp": "2026-03-01T12:00:00+00:00"},
    ]
    with patch("llogr.routes.logs.list_batch_keys", new_callable=AsyncMock, return_value=mock_files):
        resp = log_client.get("/api/public/logs/list", headers=auth_headers)
    assert resp.status_code == 200
    assert resp.json()["files"] == mock_files


def test_list_logs_passes_filters(log_client: TestClient, auth_headers: dict):
    with patch("llogr.routes.logs.list_batch_keys", new_callable=AsyncMock, return_value=[]) as mock_list:
        log_client.get(
            "/api/public/logs/list?session_id=sess1&trace_id=tr1&input_hash=aabb0011&trace_type=chatbot",
            headers=auth_headers,
        )
    _, kwargs = mock_list.call_args
    assert kwargs["session_id"] == "sess1"
    assert kwargs["trace_id"] == "tr1"
    assert kwargs["input_hash"] == "aabb0011"
    assert kwargs["trace_type"] == "chatbot"


def test_post_urls_endpoint(log_client: TestClient, auth_headers: dict):
    mock_result = [{"key": "pk-test/a.jsonl", "url": "https://presigned"}]
    with patch("llogr.routes.logs.generate_presigned_urls", new_callable=AsyncMock, return_value=mock_result):
        resp = log_client.post(
            "/api/public/logs/urls",
            json={"keys": ["pk-test/a.jsonl"]},
            headers=auth_headers,
        )
    assert resp.status_code == 200
    assert resp.json()["files"] == mock_result


def test_post_urls_rejects_foreign_keys(log_client: TestClient, auth_headers: dict):
    """generate_presigned_urls skips keys not belonging to the auth user."""
    with patch("llogr.routes.logs.generate_presigned_urls", new_callable=AsyncMock, return_value=[]) as mock_gen:
        resp = log_client.post(
            "/api/public/logs/urls",
            json={"keys": ["other-pk/evil.jsonl"]},
            headers=auth_headers,
        )
    assert resp.status_code == 200
    assert resp.json()["files"] == []


def test_ui_returns_html(log_client: TestClient):
    resp = log_client.get("/")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]
    assert "llogr" in resp.text
