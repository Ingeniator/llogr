import httpx
import pytest
import respx

from llogr.auth import AuthContext
from llogr.clickbeat import transform_events, send_to_clickbeat
from llogr.config import S3Config, ClickbeatConfig, Settings
from llogr.models import IngestionEvent

API_URL = "https://api.clickbeat.example.com/v1/events"


@pytest.fixture
def auth() -> AuthContext:
    return AuthContext(public_key="pk-test", secret_key="sk-test")


@pytest.fixture
def settings() -> Settings:
    return Settings(
        s3=S3Config(
            bucket="b", region="r", endpoint=None,
            access_key_id="a", secret_access_key="s",
        ),
        clickbeat=ClickbeatConfig(api_url=API_URL, api_key="test-key"),
    )


@pytest.fixture
def sample_events() -> list[IngestionEvent]:
    return [
        IngestionEvent(
            id="evt-1",
            timestamp="2026-01-01T00:00:00Z",
            type="trace-create",
            body={"name": "test"},
        ),
        IngestionEvent(
            id="evt-2",
            timestamp="2026-01-01T00:00:01Z",
            type="span-create",
            body={"name": "span"},
        ),
    ]


def test_transform_events(auth: AuthContext, sample_events: list[IngestionEvent]) -> None:
    result = transform_events(sample_events, auth)

    assert len(result) == 2
    assert result[0] == {
        "event_id": "evt-1",
        "event_type": "trace-create",
        "timestamp": "2026-01-01T00:00:00Z",
        "project_id": "pk-test",
        "payload": {"name": "test"},
    }
    assert result[1]["event_id"] == "evt-2"
    assert result[1]["event_type"] == "span-create"
    assert result[1]["project_id"] == "pk-test"


@respx.mock
@pytest.mark.asyncio
async def test_send_to_clickbeat(
    auth: AuthContext, settings: Settings, sample_events: list[IngestionEvent]
) -> None:
    route = respx.post(API_URL).mock(return_value=httpx.Response(200))

    await send_to_clickbeat(sample_events, auth, settings)

    assert route.called
    request = route.calls[0].request
    assert request.headers["authorization"] == "Bearer test-key"


@respx.mock
@pytest.mark.asyncio
async def test_send_to_clickbeat_error(
    auth: AuthContext, settings: Settings, sample_events: list[IngestionEvent]
) -> None:
    respx.post(API_URL).mock(return_value=httpx.Response(500))

    with pytest.raises(httpx.HTTPStatusError):
        await send_to_clickbeat(sample_events, auth, settings)
