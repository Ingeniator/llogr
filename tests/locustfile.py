"""Locust load test for llogr ingestion endpoint.

Run:
    make perf              # headless, 20 rps for 30s
    make perf-ui           # web UI at http://localhost:8089

Requires the server running at http://localhost:8000 (make dev).
"""

import uuid

from locust import HttpUser, between, task


def _make_batch(n: int = 5) -> dict:
    trace_id = str(uuid.uuid4())
    events = [
        {
            "id": str(uuid.uuid4()),
            "timestamp": "2026-03-05T12:00:00.000Z",
            "type": "trace-create",
            "body": {
                "id": trace_id,
                "name": "load-test-trace",
                "userId": "user-loadtest",
                "environment": "test",
                "tags": ["perf"],
            },
        },
    ]
    for i in range(n - 1):
        events.append({
            "id": str(uuid.uuid4()),
            "timestamp": "2026-03-05T12:00:00.100Z",
            "type": "generation-create",
            "body": {
                "id": str(uuid.uuid4()),
                "traceId": trace_id,
                "name": f"gen-{i}",
                "model": "gpt-4o",
                "startTime": "2026-03-05T12:00:00.100Z",
                "endTime": "2026-03-05T12:00:01.000Z",
                "input": [{"role": "user", "content": "Hello"}],
                "output": {"role": "assistant", "content": "Hi there!"},
                "usageDetails": {"input": 10, "output": 5, "total": 15},
            },
        })
    return {"batch": events, "metadata": {"sdk_name": "locust-perf"}}


class IngestionUser(HttpUser):
    wait_time = between(0.01, 0.05)

    def on_start(self) -> None:
        import base64
        creds = base64.b64encode(b"pk-perf:sk-perf").decode()
        self.headers = {
            "Authorization": f"Basic {creds}",
            "Content-Type": "application/json",
        }

    @task
    def ingest_batch(self) -> None:
        self.client.post(
            "/api/public/ingestion",
            json=_make_batch(),
            headers=self.headers,
        )
