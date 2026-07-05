from unittest.mock import patch

from fastapi.testclient import TestClient

from llogr.config import ClickHouseConfig, FeaturesConfig, S3Config, ServerConfig, Settings

# /health and /ready read the module-level `llogr.main.settings`, which is bound
# once at import time from config.yaml — pointing at real (unreachable in tests)
# S3/ClickHouse endpoints. Patch it per-test so these checks don't depend on a
# live backend.
_NO_BACKEND_SETTINGS = Settings(
    s3=S3Config(bucket="b", region="r", endpoint=None, access_key_id="a", secret_access_key="s"),
    clickstream=(),
    server=ServerConfig(),
    features=FeaturesConfig(store_backends=()),
    clickhouse=ClickHouseConfig(),
)


def test_health(client: TestClient) -> None:
    with patch("llogr.main.settings", _NO_BACKEND_SETTINGS):
        resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {
        "status": "ok",
        "components": {"s3": "disabled", "clickhouse": "disabled"},
    }


def test_livez(client: TestClient) -> None:
    resp = client.get("/livez")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_ready_no_backends(client: TestClient) -> None:
    with patch("llogr.main.settings", _NO_BACKEND_SETTINGS):
        resp = client.get("/ready")
    assert resp.status_code == 200
