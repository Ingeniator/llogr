"""Tests with realistic Langfuse SDK payloads for each event type."""

from fastapi.testclient import TestClient


TRACE_CREATE = {
    "id": "evt-trace-001",
    "timestamp": "2026-03-05T12:00:00.000Z",
    "type": "trace-create",
    "body": {
        "id": "trace-11111111-1111-1111-1111-111111111111",
        "timestamp": "2026-03-05T12:00:00.000Z",
        "name": "chat-completion-pipeline",
        "userId": "user-abc123",
        "sessionId": "session-xyz789",
        "release": "v2.3.1",
        "version": "1.0.0",
        "environment": "production",
        "input": {"messages": [{"role": "user", "content": "What is the capital of France?"}]},
        "output": {"content": "The capital of France is Paris."},
        "metadata": {"request_id": "req-9876"},
        "tags": ["chat", "geography", "production"],
        "public": False,
    },
}

SPAN_CREATE = {
    "id": "evt-span-001",
    "timestamp": "2026-03-05T12:00:00.100Z",
    "type": "span-create",
    "body": {
        "id": "span-22222222-2222-2222-2222-222222222222",
        "traceId": "trace-11111111-1111-1111-1111-111111111111",
        "parentObservationId": None,
        "name": "retrieval-step",
        "startTime": "2026-03-05T12:00:00.100Z",
        "endTime": "2026-03-05T12:00:00.350Z",
        "level": "DEFAULT",
        "statusMessage": None,
        "input": {"query": "capital of France", "top_k": 5},
        "output": {"documents": [{"id": "doc-1", "text": "Paris is the capital of France."}]},
        "metadata": {"vector_store": "pinecone"},
        "environment": "production",
    },
}

SPAN_UPDATE = {
    "id": "evt-span-002",
    "timestamp": "2026-03-05T12:00:00.360Z",
    "type": "span-update",
    "body": {
        "id": "span-22222222-2222-2222-2222-222222222222",
        "traceId": "trace-11111111-1111-1111-1111-111111111111",
        "endTime": "2026-03-05T12:00:00.360Z",
        "output": {"documents": [{"id": "doc-1", "text": "Paris is the capital of France."}]},
    },
}

GENERATION_CREATE = {
    "id": "evt-gen-001",
    "timestamp": "2026-03-05T12:00:00.400Z",
    "type": "generation-create",
    "body": {
        "id": "gen-33333333-3333-3333-3333-333333333333",
        "traceId": "trace-11111111-1111-1111-1111-111111111111",
        "parentObservationId": "span-22222222-2222-2222-2222-222222222222",
        "name": "openai-chat",
        "startTime": "2026-03-05T12:00:00.400Z",
        "endTime": "2026-03-05T12:00:01.200Z",
        "completionStartTime": "2026-03-05T12:00:00.950Z",
        "model": "gpt-4o",
        "modelParameters": {"temperature": 0.7, "max_tokens": 512, "top_p": 1.0},
        "input": [
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": "What is the capital of France?"},
        ],
        "output": {"role": "assistant", "content": "The capital of France is Paris."},
        "usage": {"input": 28, "output": 10, "total": 38, "unit": "TOKENS"},
        "usageDetails": {"input": 28, "output": 10, "total": 38},
        "costDetails": {"input": 0.000084, "output": 0.00006, "total": 0.000144},
        "promptName": "chat-assistant-v2",
        "promptVersion": 3,
        "level": "DEFAULT",
        "environment": "production",
        "metadata": {"provider": "openai", "finish_reason": "stop"},
    },
}

GENERATION_UPDATE = {
    "id": "evt-gen-002",
    "timestamp": "2026-03-05T12:00:01.210Z",
    "type": "generation-update",
    "body": {
        "id": "gen-33333333-3333-3333-3333-333333333333",
        "traceId": "trace-11111111-1111-1111-1111-111111111111",
        "endTime": "2026-03-05T12:00:01.200Z",
        "output": {"role": "assistant", "content": "The capital of France is Paris."},
        "usage": {"input": 28, "output": 10, "total": 38, "unit": "TOKENS"},
    },
}

EVENT_CREATE = {
    "id": "evt-event-001",
    "timestamp": "2026-03-05T12:00:00.500Z",
    "type": "event-create",
    "body": {
        "id": "evt-44444444-4444-4444-4444-444444444444",
        "traceId": "trace-11111111-1111-1111-1111-111111111111",
        "parentObservationId": "span-22222222-2222-2222-2222-222222222222",
        "name": "cache-hit",
        "startTime": "2026-03-05T12:00:00.500Z",
        "level": "DEBUG",
        "input": {"cache_key": "query:capital-of-france"},
        "output": {"hit": True},
        "metadata": {"cache_backend": "redis"},
        "environment": "production",
    },
}

SCORE_CREATE_NUMERIC = {
    "id": "evt-score-001",
    "timestamp": "2026-03-05T12:00:02.000Z",
    "type": "score-create",
    "body": {
        "id": "score-55555555-5555-5555-5555-555555555555",
        "traceId": "trace-11111111-1111-1111-1111-111111111111",
        "observationId": "gen-33333333-3333-3333-3333-333333333333",
        "name": "faithfulness",
        "value": 0.95,
        "dataType": "NUMERIC",
        "comment": "Answer is accurate and grounded in retrieved context.",
        "environment": "production",
    },
}

SCORE_CREATE_CATEGORICAL = {
    "id": "evt-score-002",
    "timestamp": "2026-03-05T12:00:02.100Z",
    "type": "score-create",
    "body": {
        "id": "score-66666666-6666-6666-6666-666666666666",
        "traceId": "trace-11111111-1111-1111-1111-111111111111",
        "name": "toxicity",
        "value": "none",
        "dataType": "CATEGORICAL",
        "comment": "No toxic content detected.",
    },
}

SDK_LOG = {
    "id": "evt-sdk-001",
    "timestamp": "2026-03-05T12:00:00.050Z",
    "type": "sdk-log",
    "body": {
        "log": {
            "level": "WARNING",
            "message": "Flush called before all events were sent; retrying.",
            "sdk": "langfuse-python",
            "version": "2.36.0",
            "extra": {"pending_events": 3, "retry_attempt": 1},
        }
    },
}


def _post_batch(client: TestClient, auth_headers: dict, events: list[dict]) -> dict:
    resp = client.post(
        "/api/public/ingestion",
        json={"batch": events},
        headers=auth_headers,
    )
    assert resp.status_code == 207
    data = resp.json()
    assert data["errors"] == []
    assert len(data["successes"]) == len(events)
    return data


class TestIndividualEventTypes:
    """Each Langfuse event type is accepted individually."""

    def test_trace_create(self, client: TestClient, auth_headers: dict) -> None:
        _post_batch(client, auth_headers, [TRACE_CREATE])

    def test_span_create(self, client: TestClient, auth_headers: dict) -> None:
        _post_batch(client, auth_headers, [SPAN_CREATE])

    def test_span_update(self, client: TestClient, auth_headers: dict) -> None:
        _post_batch(client, auth_headers, [SPAN_UPDATE])

    def test_generation_create(self, client: TestClient, auth_headers: dict) -> None:
        _post_batch(client, auth_headers, [GENERATION_CREATE])

    def test_generation_update(self, client: TestClient, auth_headers: dict) -> None:
        _post_batch(client, auth_headers, [GENERATION_UPDATE])

    def test_event_create(self, client: TestClient, auth_headers: dict) -> None:
        _post_batch(client, auth_headers, [EVENT_CREATE])

    def test_score_create_numeric(self, client: TestClient, auth_headers: dict) -> None:
        _post_batch(client, auth_headers, [SCORE_CREATE_NUMERIC])

    def test_score_create_categorical(self, client: TestClient, auth_headers: dict) -> None:
        _post_batch(client, auth_headers, [SCORE_CREATE_CATEGORICAL])

    def test_sdk_log(self, client: TestClient, auth_headers: dict) -> None:
        _post_batch(client, auth_headers, [SDK_LOG])


class TestMixedBatch:
    """Simulates a real SDK flush with a full trace lifecycle in one batch."""

    def test_full_trace_lifecycle(self, client: TestClient, auth_headers: dict) -> None:
        """Trace → span → generation → score, as the SDK would send them."""
        events = [
            TRACE_CREATE,
            SPAN_CREATE,
            GENERATION_CREATE,
            SCORE_CREATE_NUMERIC,
        ]
        data = _post_batch(client, auth_headers, events)
        ids = {s["id"] for s in data["successes"]}
        assert ids == {"evt-trace-001", "evt-span-001", "evt-gen-001", "evt-score-001"}

    def test_create_then_update(self, client: TestClient, auth_headers: dict) -> None:
        """Create + update in the same batch (common pattern)."""
        events = [SPAN_CREATE, SPAN_UPDATE, GENERATION_CREATE, GENERATION_UPDATE]
        _post_batch(client, auth_headers, events)

    def test_all_event_types_in_one_batch(self, client: TestClient, auth_headers: dict) -> None:
        """Every supported event type in a single batch."""
        events = [
            TRACE_CREATE,
            SPAN_CREATE,
            SPAN_UPDATE,
            GENERATION_CREATE,
            GENERATION_UPDATE,
            EVENT_CREATE,
            SCORE_CREATE_NUMERIC,
            SCORE_CREATE_CATEGORICAL,
            SDK_LOG,
        ]
        data = _post_batch(client, auth_headers, events)
        assert len(data["successes"]) == 9


class TestBatchMetadata:
    """The optional metadata field on the batch envelope."""

    def test_with_sdk_metadata(self, client: TestClient, auth_headers: dict) -> None:
        resp = client.post(
            "/api/public/ingestion",
            json={
                "batch": [TRACE_CREATE],
                "metadata": {
                    "batch_size": 1,
                    "sdk_integration": "langchain",
                    "sdk_name": "langfuse-python",
                    "sdk_version": "2.36.0",
                    "public_key": "pk-test",
                },
            },
            headers=auth_headers,
        )
        assert resp.status_code == 207

    def test_without_metadata(self, client: TestClient, auth_headers: dict) -> None:
        resp = client.post(
            "/api/public/ingestion",
            json={"batch": [TRACE_CREATE]},
            headers=auth_headers,
        )
        assert resp.status_code == 207
