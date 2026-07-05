"""Tests for the /api/public/export streaming endpoint."""
from __future__ import annotations

import base64
from contextlib import contextmanager
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from llogr.config import ClickHouseConfig, get_settings
from llogr.main import app


@pytest.fixture
def client():
    yield TestClient(app)


@pytest.fixture
def auth_headers() -> dict[str, str]:
    creds = base64.b64encode(b"myorg/alice:sk-test").decode()
    return {"Authorization": f"Basic {creds}"}


def _gen(*lines: str):
    """Async generator that yields lines."""
    async def _agen():
        for line in lines:
            yield line
    return _agen()


@contextmanager
def _with_clickhouse_url(url: str = "http://clickhouse:8123"):
    """Override get_settings dependency to inject a clickhouse URL."""
    real = get_settings()
    fake_settings = type(real)(
        s3=real.s3,
        clickstream=real.clickstream,
        server=real.server,
        features=real.features,
        clickhouse=ClickHouseConfig(url=url),
    )
    app.dependency_overrides[get_settings] = lambda: fake_settings
    try:
        yield fake_settings
    finally:
        app.dependency_overrides.pop(get_settings, None)


def test_export_requires_start_and_end(client, auth_headers):
    resp = client.get("/api/public/export", headers=auth_headers)
    assert resp.status_code == 422


def test_export_rejects_end_before_start(client, auth_headers):
    with _with_clickhouse_url():
        resp = client.get(
            "/api/public/export",
            params={"start": "2026-05-10T00:00:00", "end": "2026-05-01T00:00:00"},
            headers=auth_headers,
        )
    assert resp.status_code == 400
    assert "end must be after start" in resp.json()["detail"]


def test_export_returns_503_when_clickhouse_not_configured(client, auth_headers):
    resp = client.get(
        "/api/public/export",
        params={"start": "2026-05-01T00:00:00", "end": "2026-05-19T23:59:59"},
        headers=auth_headers,
    )
    assert resp.status_code == 503


def test_export_streams_jsonl_lines(client, auth_headers):
    line1 = '{"event_id":"a","model":"gpt-4o","body":"{}"}'
    line2 = '{"event_id":"b","model":"gpt-4o","body":"{}"}'

    with _with_clickhouse_url():
        with patch("llogr.clickhouse.export_generations_ch", return_value=_gen(line1 + "\n", line2 + "\n")):
            resp = client.get(
                "/api/public/export",
                params={"start": "2026-05-01T00:00:00", "end": "2026-05-19T23:59:59"},
                headers=auth_headers,
            )

    assert resp.status_code == 200
    assert resp.headers["content-type"] == "application/x-ndjson"
    assert "attachment" in resp.headers.get("content-disposition", "")
    lines = [ln for ln in resp.text.splitlines() if ln.strip()]
    assert len(lines) == 2
    assert "event_id" in lines[0]


def test_export_passes_auth_project_id_to_clickhouse(auth_headers):
    captured = {}

    async def _fake_export(project_id, settings, start, end, is_org_admin=False, is_super_admin=False, session_id=None):
        captured["project_id"] = project_id
        captured["is_org_admin"] = is_org_admin
        yield '{"event_id":"x"}\n'

    with _with_clickhouse_url():
        with patch("llogr.clickhouse.export_generations_ch", side_effect=_fake_export):
            client = TestClient(app)
            client.get(
                "/api/public/export",
                params={"start": "2026-05-01T00:00:00", "end": "2026-05-19T23:59:59"},
                headers=auth_headers,
            )

    assert captured["project_id"] == "myorg/alice"
    assert captured["is_org_admin"] is False


def test_export_org_admin_sets_flag():
    # ORG_ADMIN role is propagated via x-group-id + x-role headers (nginx path)
    headers = {
        "x-group-id": "myorg/admin",
        "x-role": "ORG_ADMIN",
    }

    captured = {}

    async def _fake_export(project_id, settings, start, end, is_org_admin=False, is_super_admin=False, session_id=None):
        captured["is_org_admin"] = is_org_admin
        yield '{"event_id":"x"}\n'

    with _with_clickhouse_url():
        with patch("llogr.clickhouse.export_generations_ch", side_effect=_fake_export):
            client = TestClient(app)
            client.get(
                "/api/public/export",
                params={"start": "2026-05-01T00:00:00", "end": "2026-05-19T23:59:59"},
                headers=headers,
            )

    assert captured["is_org_admin"] is True
