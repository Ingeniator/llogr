import base64

from fastapi.testclient import TestClient


def _make_event(id: str = "evt-1", type: str = "trace-create", body: dict | None = None) -> dict:
    return {
        "id": id,
        "timestamp": "2026-01-01T00:00:00Z",
        "type": type,
        "body": body or {"name": "test"},
    }


def test_ingest_single_event(client: TestClient, auth_headers: dict[str, str]) -> None:
    resp = client.post(
        "/api/public/ingestion",
        json={"batch": [_make_event()]},
        headers=auth_headers,
    )
    assert resp.status_code == 207
    data = resp.json()
    assert len(data["successes"]) == 1
    assert data["successes"][0]["id"] == "evt-1"
    assert data["successes"][0]["status"] == 201
    assert data["errors"] == []


def test_ingest_multiple_events(client: TestClient, auth_headers: dict[str, str]) -> None:
    events = [_make_event(id=f"evt-{i}", type=t) for i, t in enumerate([
        "trace-create", "span-create", "generation-create", "score-create",
    ])]
    resp = client.post(
        "/api/public/ingestion",
        json={"batch": events},
        headers=auth_headers,
    )
    assert resp.status_code == 207
    data = resp.json()
    assert len(data["successes"]) == 4
    assert {s["id"] for s in data["successes"]} == {"evt-0", "evt-1", "evt-2", "evt-3"}


def test_ingest_with_metadata(client: TestClient, auth_headers: dict[str, str]) -> None:
    resp = client.post(
        "/api/public/ingestion",
        json={"batch": [_make_event()], "metadata": {"sdk_version": "1.0"}},
        headers=auth_headers,
    )
    assert resp.status_code == 207


def test_ingest_empty_batch(client: TestClient, auth_headers: dict[str, str]) -> None:
    resp = client.post(
        "/api/public/ingestion",
        json={"batch": []},
        headers=auth_headers,
    )
    assert resp.status_code == 207
    assert resp.json()["successes"] == []


def test_ingest_invalid_event_type(client: TestClient, auth_headers: dict[str, str]) -> None:
    resp = client.post(
        "/api/public/ingestion",
        json={"batch": [_make_event(type="invalid-type")]},
        headers=auth_headers,
    )
    assert resp.status_code == 422
