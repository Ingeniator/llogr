import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from llogr.auth import AuthContext
from llogr.config import ClickbeatConfig, S3Config, Settings
from llogr.models import IngestionEvent
from llogr.s3 import (
    extract_input_hash,
    extract_trace_id,
    extract_trace_type,
    sanitize_trace_type,
    save_batch_to_s3,
)

BUCKET = "test-bucket"
REGION = "us-east-1"


@pytest.fixture
def settings() -> Settings:
    return Settings(
        s3=S3Config(
            bucket=BUCKET,
            region=REGION,
            endpoint=None,
            access_key_id="testing",
            secret_access_key="testing",
        ),
        clickbeat=ClickbeatConfig(api_url="http://x", api_key="k"),
    )


@pytest.fixture
def auth() -> AuthContext:
    return AuthContext(public_key="pk-test", secret_key="sk-test")


@pytest.fixture
def sample_batch() -> list[IngestionEvent]:
    return [
        IngestionEvent(
            id="evt-1",
            timestamp="2026-01-01T00:00:00Z",
            type="trace-create",
            body={"name": "test-trace"},
        ),
    ]


# --- extract helpers ---


def test_extract_trace_id_from_trace_create():
    batch = [
        IngestionEvent(id="trace-abc", timestamp="t", type="trace-create", body={}),
    ]
    assert extract_trace_id(batch) == "trace-abc"


def test_extract_trace_id_from_body():
    batch = [
        IngestionEvent(id="evt-1", timestamp="t", type="generation-create", body={"traceId": "tr-99"}),
    ]
    assert extract_trace_id(batch) == "tr-99"


def test_extract_trace_id_unknown():
    batch = [
        IngestionEvent(id="evt-1", timestamp="t", type="sdk-log", body={}),
    ]
    assert extract_trace_id(batch) == "unknown"


def test_extract_input_hash_with_input():
    batch = [
        IngestionEvent(id="evt-1", timestamp="t", type="generation-create", body={"input": "hello world"}),
    ]
    h = extract_input_hash(batch)
    assert len(h) == 8
    # Same input produces same hash
    assert extract_input_hash(batch) == h


def test_extract_input_hash_no_input():
    batch = [
        IngestionEvent(id="evt-1", timestamp="t", type="trace-create", body={}),
    ]
    assert extract_input_hash(batch) == "noinput"


def test_extract_trace_type_from_trace_create():
    batch = [
        IngestionEvent(id="evt-1", timestamp="t", type="trace-create", body={"name": "my-trace"}),
    ]
    assert extract_trace_type(batch) == "my-trace"


def test_extract_trace_type_unknown():
    batch = [
        IngestionEvent(id="evt-1", timestamp="t", type="sdk-log", body={}),
    ]
    assert extract_trace_type(batch) == "unknown"


def test_sanitize_trace_type():
    assert sanitize_trace_type("hello_world") == "hello-world"
    assert sanitize_trace_type("no-underscores") == "no-underscores"
    assert sanitize_trace_type("a_b_c") == "a-b-c"


# --- save_batch_to_s3 ---


@pytest.mark.asyncio
async def test_save_batch_to_s3(
    settings: Settings, auth: AuthContext, sample_batch: list[IngestionEvent]
) -> None:
    mock_put = AsyncMock()
    mock_client = AsyncMock()
    mock_client.put_object = mock_put
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    mock_session = MagicMock()
    mock_session.client.return_value = mock_client

    with patch("llogr.s3.aioboto3.Session", return_value=mock_session):
        key = await save_batch_to_s3(sample_batch, auth, settings)

    assert key.startswith("pk-test/")
    assert key.endswith(".jsonl")
    # Key format: pk-test/{session}_{trace}_{tracetype}_{hash}_{ts}_{uuid}.jsonl
    filename = key.split("/", 1)[1]
    parts = filename.removesuffix(".jsonl").split("_")
    assert len(parts) >= 6  # session, trace, tracetype, hash, ts, uuid

    mock_put.assert_called_once()
    call_kwargs = mock_put.call_args.kwargs
    assert call_kwargs["Bucket"] == BUCKET
    assert call_kwargs["Key"] == key
    assert call_kwargs["ContentType"] == "application/x-ndjson"

    lines = call_kwargs["Body"].decode().strip().split("\n")
    assert len(lines) == 1
    event = json.loads(lines[0])
    assert event["id"] == "evt-1"
    assert event["type"] == "trace-create"


@pytest.mark.asyncio
async def test_save_batch_with_session_id(
    settings: Settings, auth: AuthContext, sample_batch: list[IngestionEvent]
) -> None:
    mock_put = AsyncMock()
    mock_client = AsyncMock()
    mock_client.put_object = mock_put
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    mock_session = MagicMock()
    mock_session.client.return_value = mock_client

    with patch("llogr.s3.aioboto3.Session", return_value=mock_session):
        key = await save_batch_to_s3(sample_batch, auth, settings, session_id="sess-42")

    filename = key.split("/", 1)[1]
    assert filename.startswith("sess-42_")


@pytest.mark.asyncio
async def test_save_batch_multiple_events(
    settings: Settings, auth: AuthContext
) -> None:
    mock_put = AsyncMock()
    mock_client = AsyncMock()
    mock_client.put_object = mock_put
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    mock_session = MagicMock()
    mock_session.client.return_value = mock_client

    batch = [
        IngestionEvent(
            id=f"evt-{i}",
            timestamp="2026-01-01T00:00:00Z",
            type="trace-create",
            body={"name": f"trace-{i}"},
        )
        for i in range(3)
    ]

    with patch("llogr.s3.aioboto3.Session", return_value=mock_session):
        key = await save_batch_to_s3(batch, auth, settings)

    lines = mock_put.call_args.kwargs["Body"].decode().strip().split("\n")
    assert len(lines) == 3
    events = [json.loads(line) for line in lines]
    assert {e["id"] for e in events} == {"evt-0", "evt-1", "evt-2"}
