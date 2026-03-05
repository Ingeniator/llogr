import base64
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from llogr.main import app


@pytest.fixture
def client() -> TestClient:
    with patch("llogr.routes.ingestion.stage1_save_raw", new_callable=AsyncMock) as mock_s1, \
         patch("llogr.routes.ingestion.stage2_forward_to_clickbeat", new_callable=AsyncMock) as mock_s2:
        yield TestClient(app)


@pytest.fixture
def auth_headers() -> dict[str, str]:
    creds = base64.b64encode(b"pk-test:sk-test").decode()
    return {"Authorization": f"Basic {creds}"}
