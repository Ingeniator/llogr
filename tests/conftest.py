import base64
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from llogr.main import app


@pytest.fixture
def client() -> TestClient:
    with patch("llogr.routes.ingestion.ingest", new_callable=AsyncMock):
        yield TestClient(app)


@pytest.fixture
def auth_headers() -> dict[str, str]:
    creds = base64.b64encode(b"pk-test:sk-test").decode()
    return {"Authorization": f"Basic {creds}"}
