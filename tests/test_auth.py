import base64

from fastapi.testclient import TestClient


def _make_batch() -> dict:
    return {"batch": [{"id": "e1", "timestamp": "2026-01-01T00:00:00Z", "type": "trace-create", "body": {}}]}


def test_no_auth_header(client: TestClient) -> None:
    resp = client.post("/api/public/ingestion", json=_make_batch())
    assert resp.status_code == 401


def test_invalid_auth_scheme(client: TestClient) -> None:
    resp = client.post(
        "/api/public/ingestion",
        json=_make_batch(),
        headers={"Authorization": "Bearer token123"},
    )
    assert resp.status_code == 401


def test_malformed_base64(client: TestClient) -> None:
    resp = client.post(
        "/api/public/ingestion",
        json=_make_batch(),
        headers={"Authorization": "Basic !!!notbase64"},
    )
    assert resp.status_code == 401


def test_empty_public_key(client: TestClient) -> None:
    creds = base64.b64encode(b":sk-test").decode()
    resp = client.post(
        "/api/public/ingestion",
        json=_make_batch(),
        headers={"Authorization": f"Basic {creds}"},
    )
    assert resp.status_code == 401


def test_empty_secret_key_still_works(client: TestClient) -> None:
    """Secret key is optional in Basic auth (JWT subject may not be present)."""
    creds = base64.b64encode(b"pk-test:").decode()
    resp = client.post(
        "/api/public/ingestion",
        json=_make_batch(),
        headers={"Authorization": f"Basic {creds}"},
    )
    assert resp.status_code == 207


def test_jwt_headers(client: TestClient) -> None:
    """X-Auth-Tenant/Subject headers take priority over Basic auth."""
    resp = client.post(
        "/api/public/ingestion",
        json=_make_batch(),
        headers={"X-Auth-Tenant": "tenant-1", "X-Auth-Subject": "user-1"},
    )
    assert resp.status_code == 207


def test_valid_auth(client: TestClient, auth_headers: dict[str, str]) -> None:
    resp = client.post("/api/public/ingestion", json=_make_batch(), headers=auth_headers)
    assert resp.status_code == 207
