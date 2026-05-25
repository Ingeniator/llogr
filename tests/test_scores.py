"""Tests for POST /api/public/scores — Langfuse REST API score endpoint."""
from __future__ import annotations

import base64
from unittest.mock import AsyncMock, call, patch

import pytest
from fastapi.testclient import TestClient

from llogr.main import app


@pytest.fixture
def auth_headers() -> dict[str, str]:
    creds = base64.b64encode(b"pk-test:sk-test").decode()
    return {"Authorization": f"Basic {creds}"}


@pytest.fixture
def scores_client():
    """TestClient with llogr.routes.scores.ingest mocked out. Yields (client, mock_ingest)."""
    with patch("llogr.routes.scores.ingest", new_callable=AsyncMock, return_value=[]) as mock_ingest:
        yield TestClient(app), mock_ingest


# ── happy path ────────────────────────────────────────────────────────────────

class TestCreateScore:
    def test_minimal_numeric_score(self, scores_client, auth_headers):
        client, _ = scores_client
        resp = client.post(
            "/api/public/scores",
            json={"traceId": "trace-abc", "name": "quality", "value": 0.9},
            headers=auth_headers,
        )
        assert resp.status_code == 201
        data = resp.json()
        assert "id" in data
        assert isinstance(data["id"], str)
        assert len(data["id"]) > 0

    def test_categorical_score_string_value(self, scores_client, auth_headers):
        client, _ = scores_client
        resp = client.post(
            "/api/public/scores",
            json={
                "traceId": "trace-abc",
                "name": "toxicity",
                "value": "none",
                "dataType": "CATEGORICAL",
            },
            headers=auth_headers,
        )
        assert resp.status_code == 201

    def test_all_optional_fields(self, scores_client, auth_headers):
        client, _ = scores_client
        resp = client.post(
            "/api/public/scores",
            json={
                "traceId": "trace-abc",
                "name": "faithfulness",
                "value": 0.95,
                "dataType": "NUMERIC",
                "comment": "Grounded in context.",
                "observationId": "gen-xyz",
                "configId": "cfg-001",
                "id": "score-custom-id",
            },
            headers=auth_headers,
        )
        assert resp.status_code == 201
        assert resp.json()["id"] == "score-custom-id"

    def test_custom_id_is_preserved(self, scores_client, auth_headers):
        client, _ = scores_client
        resp = client.post(
            "/api/public/scores",
            json={"traceId": "t", "name": "n", "value": 1.0, "id": "my-fixed-id"},
            headers=auth_headers,
        )
        assert resp.status_code == 201
        assert resp.json()["id"] == "my-fixed-id"

    def test_auto_generates_id_when_absent(self, scores_client, auth_headers):
        client, _ = scores_client
        r1 = client.post(
            "/api/public/scores",
            json={"traceId": "t", "name": "n", "value": 1.0},
            headers=auth_headers,
        )
        r2 = client.post(
            "/api/public/scores",
            json={"traceId": "t", "name": "n", "value": 1.0},
            headers=auth_headers,
        )
        assert r1.json()["id"] != r2.json()["id"]


# ── ingestion event shape ─────────────────────────────────────────────────────

class TestIngestionEventShape:
    def test_routes_as_score_create_event(self, scores_client, auth_headers):
        client, mock_ingest = scores_client
        client.post(
            "/api/public/scores",
            json={"traceId": "trace-1", "name": "accuracy", "value": 0.8},
            headers=auth_headers,
        )
        mock_ingest.assert_awaited_once()
        events, auth = mock_ingest.call_args[0]
        assert len(events) == 1
        event = events[0]
        assert event.type == "score-create"
        assert event.body["traceId"] == "trace-1"
        assert event.body["name"] == "accuracy"
        assert event.body["value"] == 0.8

    def test_optional_fields_included_in_body_when_present(self, scores_client, auth_headers):
        client, mock_ingest = scores_client
        client.post(
            "/api/public/scores",
            json={
                "traceId": "t",
                "name": "n",
                "value": 1,
                "dataType": "NUMERIC",
                "comment": "ok",
                "observationId": "obs-1",
                "configId": "cfg-1",
            },
            headers=auth_headers,
        )
        event = mock_ingest.call_args[0][0][0]
        assert event.body["dataType"] == "NUMERIC"
        assert event.body["comment"] == "ok"
        assert event.body["observationId"] == "obs-1"
        assert event.body["configId"] == "cfg-1"

    def test_optional_fields_absent_from_body_when_not_provided(self, scores_client, auth_headers):
        client, mock_ingest = scores_client
        client.post(
            "/api/public/scores",
            json={"traceId": "t", "name": "n", "value": 1.0},
            headers=auth_headers,
        )
        event = mock_ingest.call_args[0][0][0]
        assert "dataType" not in event.body
        assert "comment" not in event.body
        assert "observationId" not in event.body
        assert "configId" not in event.body

    def test_event_id_matches_score_id(self, scores_client, auth_headers):
        client, mock_ingest = scores_client
        resp = client.post(
            "/api/public/scores",
            json={"traceId": "t", "name": "n", "value": 1.0, "id": "score-42"},
            headers=auth_headers,
        )
        event = mock_ingest.call_args[0][0][0]
        assert event.id == "score-42"
        assert event.body["id"] == "score-42"
        assert resp.json()["id"] == "score-42"


# ── auth ──────────────────────────────────────────────────────────────────────

class TestAuth:
    def test_requires_authentication(self, scores_client):
        client, _ = scores_client
        resp = client.post(
            "/api/public/scores",
            json={"traceId": "t", "name": "n", "value": 1.0},
        )
        assert resp.status_code == 401

    def test_accepts_group_id_header(self, scores_client):
        client, _ = scores_client
        resp = client.post(
            "/api/public/scores",
            json={"traceId": "t", "name": "n", "value": 1.0},
            headers={"X-Group-ID": "org1/user1"},
        )
        assert resp.status_code == 201


# ── validation ────────────────────────────────────────────────────────────────

class TestValidation:
    def test_missing_trace_id_returns_422(self, scores_client, auth_headers):
        client, _ = scores_client
        resp = client.post(
            "/api/public/scores",
            json={"name": "n", "value": 1.0},
            headers=auth_headers,
        )
        assert resp.status_code == 422

    def test_missing_name_returns_422(self, scores_client, auth_headers):
        client, _ = scores_client
        resp = client.post(
            "/api/public/scores",
            json={"traceId": "t", "value": 1.0},
            headers=auth_headers,
        )
        assert resp.status_code == 422

    def test_missing_value_returns_422(self, scores_client, auth_headers):
        client, _ = scores_client
        resp = client.post(
            "/api/public/scores",
            json={"traceId": "t", "name": "n"},
            headers=auth_headers,
        )
        assert resp.status_code == 422

    def test_invalid_data_type_returns_422(self, scores_client, auth_headers):
        client, _ = scores_client
        resp = client.post(
            "/api/public/scores",
            json={"traceId": "t", "name": "n", "value": 1.0, "dataType": "INVALID"},
            headers=auth_headers,
        )
        assert resp.status_code == 422


# ── storage failure ───────────────────────────────────────────────────────────

class TestStorageFailure:
    def test_returns_500_on_ingest_failure(self, auth_headers):
        with patch("llogr.routes.scores.ingest", new_callable=AsyncMock, return_value=["s3"]):
            client = TestClient(app)
            resp = client.post(
                "/api/public/scores",
                json={"traceId": "t", "name": "n", "value": 1.0},
                headers=auth_headers,
            )
        assert resp.status_code == 500
