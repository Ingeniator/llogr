from fastapi.testclient import TestClient

from llogr.main import app
from llogr.metrics import (
    CLICKSTREAM_FORWARD_ERRORS,
    CLICKSTREAM_FORWARD_SECONDS,
    EVENTS_INGESTED,
    S3_SAVE_ERRORS,
    S3_SAVE_SECONDS,
)


def test_metrics_endpoint_returns_200():
    client = TestClient(app)

    # In prometheus multiprocess mode a metric's samples only materialize once
    # it's been observed at least once, so drive one real request (for the
    # auto-instrumented HTTP metrics) and record one sample per llogr metric
    # before scraping — independent of what other test files ran earlier.
    client.get("/livez")
    EVENTS_INGESTED.labels(project_id="test").inc()
    S3_SAVE_SECONDS.observe(0.01)
    S3_SAVE_ERRORS.inc()
    CLICKSTREAM_FORWARD_SECONDS.observe(0.01)
    CLICKSTREAM_FORWARD_ERRORS.inc()

    resp = client.get("/metrics")
    assert resp.status_code == 200
    body = resp.text
    assert "llogr_events_ingested_total" in body
    assert "llogr_s3_save_seconds" in body
    assert "llogr_s3_save_errors_total" in body
    assert "llogr_clickstream_forward_seconds" in body
    assert "llogr_clickstream_forward_errors_total" in body
    # Auto-instrumented HTTP metrics
    assert "http_request_duration" in body
    assert "http_request_size_bytes" in body
    assert "http_response_size_bytes" in body
    assert "http_requests_total" in body
