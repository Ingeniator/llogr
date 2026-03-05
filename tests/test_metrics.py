from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from llogr.main import app


def test_metrics_endpoint_returns_200():
    with patch("llogr.routes.ingestion.stage1_save_raw", new_callable=AsyncMock), \
         patch("llogr.routes.ingestion.stage2_forward_to_clickbeat", new_callable=AsyncMock):
        client = TestClient(app)
        resp = client.get("/metrics")
        assert resp.status_code == 200
        body = resp.text
        assert "llogr_events_ingested_total" in body or "llogr_events_ingested" in body
        assert "llogr_s3_save_seconds" in body
        assert "llogr_s3_save_errors_total" in body or "llogr_s3_save_errors" in body
        assert "llogr_clickbeat_forward_seconds" in body
        assert "llogr_clickbeat_forward_errors_total" in body or "llogr_clickbeat_forward_errors" in body
        # Auto-instrumented HTTP metrics
        assert "http_request_duration" in body
        assert "http_request_size_bytes" in body
        assert "http_response_size_bytes" in body
        assert "http_requests_total" in body
