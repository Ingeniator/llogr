from fastapi.testclient import TestClient


def test_health(client: TestClient) -> None:
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_livez(client: TestClient) -> None:
    resp = client.get("/livez")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_ready_no_backends(client: TestClient) -> None:
    resp = client.get("/ready")
    assert resp.status_code == 200
