import json
from io import BytesIO
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from llogr.auth import AuthContext
from llogr.config import S3Config, Settings
from llogr.routes.media import _blob_key, _meta_key


BUCKET = "test-bucket"
REGION = "us-east-1"
PRESIGN_EXPIRY = 3600

UPLOAD_BODY = {
    "traceId": "trace-123",
    "observationId": "obs-456",
    "contentType": "image/png",
    "contentLength": 1024,
    "sha256Hash": "abc123def456ghi789jkl0mn",
    "field": "input",
}

MEDIA_ID = UPLOAD_BODY["sha256Hash"][:22]  # "abc123def456ghi789jk"


@pytest.fixture
def s3_cfg() -> S3Config:
    return S3Config(
        bucket=BUCKET,
        region=REGION,
        endpoint="http://minio:9000",
        access_key_id="testing",
        secret_access_key="testing",
        presign_expiry=PRESIGN_EXPIRY,
    )


@pytest.fixture
def settings(s3_cfg: S3Config) -> Settings:
    return Settings(s3=s3_cfg)


@pytest.fixture
def auth() -> AuthContext:
    return AuthContext(public_key="pk-test", secret_key="sk-test")


def _mock_s3_client(*, head_returns_none: bool = True, meta_body: dict | None = None):
    """Build a mock S3 client with configurable head_object and get_object behavior."""
    mock_client = AsyncMock()
    mock_client.put_object = AsyncMock()
    mock_client.generate_presigned_url = AsyncMock(
        return_value="http://minio:9000/test-bucket/presigned-url"
    )

    if head_returns_none:
        err = mock_client.exceptions.ClientError
        err.side_effect = None
        exc = type("ClientError", (Exception,), {})()
        exc.response = {"Error": {"Code": "404"}}
        mock_client.head_object = AsyncMock(side_effect=exc)
        # Make exceptions.ClientError match the exception type
        mock_client.exceptions.ClientError = type(exc)
    else:
        mock_client.head_object = AsyncMock(return_value={"ContentLength": 1024})

    if meta_body is not None:
        body_stream = AsyncMock()
        body_stream.read = AsyncMock(return_value=json.dumps(meta_body).encode())
        mock_client.get_object = AsyncMock(return_value={"Body": body_stream})

    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    mock_session = MagicMock()
    mock_session.client.return_value = mock_client
    return mock_session, mock_client


# ---------------------------------------------------------------------------
# POST /api/public/media — get upload URL
# ---------------------------------------------------------------------------


def test_post_media_returns_upload_url(client, auth_headers):
    session, mock_client = _mock_s3_client(head_returns_none=True)
    with patch("llogr.routes.media._s3_session", return_value=session):
        resp = client.post("/api/public/media", json=UPLOAD_BODY, headers=auth_headers)

    assert resp.status_code == 200
    data = resp.json()
    assert data["mediaId"] == MEDIA_ID
    assert data["uploadUrl"] is not None

    # Verify metadata was saved to S3
    mock_client.put_object.assert_called_once()
    call_kwargs = mock_client.put_object.call_args.kwargs
    assert call_kwargs["ContentType"] == "application/json"
    meta = json.loads(call_kwargs["Body"])
    assert meta["traceId"] == "trace-123"
    assert meta["contentType"] == "image/png"
    assert meta["mediaId"] == MEDIA_ID

    # Verify presigned URL was generated for PUT
    mock_client.generate_presigned_url.assert_called_once_with(
        "put_object",
        Params={
            "Bucket": BUCKET,
            "Key": _blob_key("pk-test", MEDIA_ID, S3Config(
                bucket=BUCKET, region=REGION, endpoint="http://minio:9000",
                access_key_id="testing", secret_access_key="testing",
            )),
            "ContentType": "image/png",
        },
        ExpiresIn=PRESIGN_EXPIRY,
    )


def test_post_media_deduplication(client, auth_headers):
    """If blob already exists, uploadUrl should be null."""
    session, mock_client = _mock_s3_client(head_returns_none=False)
    with patch("llogr.routes.media._s3_session", return_value=session):
        resp = client.post("/api/public/media", json=UPLOAD_BODY, headers=auth_headers)

    assert resp.status_code == 200
    data = resp.json()
    assert data["mediaId"] == MEDIA_ID
    assert data["uploadUrl"] is None

    # No metadata saved, no presigned URL generated
    mock_client.put_object.assert_not_called()
    mock_client.generate_presigned_url.assert_not_called()


def test_post_media_requires_auth(client):
    resp = client.post("/api/public/media", json=UPLOAD_BODY)
    assert resp.status_code == 401


def test_post_media_public_endpoint_replacement(client, auth_headers):
    """When public_endpoint is set, presigned URL should use it."""
    session, mock_client = _mock_s3_client(head_returns_none=True)
    mock_client.generate_presigned_url = AsyncMock(
        return_value="http://minio:9000/test-bucket/presigned-url"
    )

    cfg = S3Config(
        bucket=BUCKET,
        region=REGION,
        endpoint="http://minio:9000",
        access_key_id="testing",
        secret_access_key="testing",
        public_endpoint="https://s3.example.com",
    )
    settings = Settings(s3=cfg)

    with (
        patch("llogr.routes.media._s3_session", return_value=session),
        patch("llogr.routes.media.get_settings", return_value=settings),
    ):
        resp = client.post("/api/public/media", json=UPLOAD_BODY, headers=auth_headers)

    assert resp.status_code == 200
    data = resp.json()
    assert "s3.example.com" in data["uploadUrl"]
    assert "minio:9000" not in data["uploadUrl"]


# ---------------------------------------------------------------------------
# PATCH /api/public/media/{media_id} — confirm upload
# ---------------------------------------------------------------------------

PATCH_BODY = {
    "uploadedAt": "2026-01-01T00:00:00Z",
    "uploadHttpStatus": 200,
    "uploadHttpError": None,
    "uploadTimeMs": 150,
}


def test_patch_media_success(client, auth_headers):
    existing_meta = {
        "mediaId": MEDIA_ID,
        "traceId": "trace-123",
        "contentType": "image/png",
        "contentLength": 1024,
    }
    session, mock_client = _mock_s3_client(head_returns_none=False, meta_body=existing_meta)
    with patch("llogr.routes.media._s3_session", return_value=session):
        resp = client.patch(
            f"/api/public/media/{MEDIA_ID}",
            json=PATCH_BODY,
            headers=auth_headers,
        )

    assert resp.status_code == 204

    # Verify metadata was updated with upload status
    mock_client.put_object.assert_called_once()
    saved = json.loads(mock_client.put_object.call_args.kwargs["Body"])
    assert saved["uploadedAt"] == "2026-01-01T00:00:00Z"
    assert saved["uploadHttpStatus"] == 200
    assert saved["uploadTimeMs"] == 150


def test_patch_media_not_found(client, auth_headers):
    session, _ = _mock_s3_client(head_returns_none=True)
    with patch("llogr.routes.media._s3_session", return_value=session):
        resp = client.patch(
            f"/api/public/media/{MEDIA_ID}",
            json=PATCH_BODY,
            headers=auth_headers,
        )

    assert resp.status_code == 404


def test_patch_media_requires_auth(client):
    resp = client.patch(f"/api/public/media/{MEDIA_ID}", json=PATCH_BODY)
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# GET /api/public/media/{media_id} — retrieve media info
# ---------------------------------------------------------------------------


def test_get_media_success(client, auth_headers):
    existing_meta = {
        "mediaId": MEDIA_ID,
        "traceId": "trace-123",
        "contentType": "image/png",
        "contentLength": 1024,
        "uploadedAt": "2026-01-01T00:00:00+00:00",
    }
    session, mock_client = _mock_s3_client(head_returns_none=False, meta_body=existing_meta)
    with patch("llogr.routes.media._s3_session", return_value=session):
        resp = client.get(
            f"/api/public/media/{MEDIA_ID}",
            headers=auth_headers,
        )

    assert resp.status_code == 200
    data = resp.json()
    assert data["mediaId"] == MEDIA_ID
    assert data["contentType"] == "image/png"
    assert data["contentLength"] == 1024
    assert data["uploadedAt"] is not None
    assert data["url"] is not None
    assert data["urlExpiry"] is not None

    # Verify presigned GET URL was generated
    mock_client.generate_presigned_url.assert_called_once()
    call_args = mock_client.generate_presigned_url.call_args
    assert call_args[0][0] == "get_object"


def test_get_media_not_found(client, auth_headers):
    session, _ = _mock_s3_client(head_returns_none=True)
    with patch("llogr.routes.media._s3_session", return_value=session):
        resp = client.get(
            f"/api/public/media/{MEDIA_ID}",
            headers=auth_headers,
        )

    assert resp.status_code == 404


def test_get_media_without_uploaded_at(client, auth_headers):
    """Before PATCH is called, uploadedAt should be null."""
    existing_meta = {
        "mediaId": MEDIA_ID,
        "traceId": "trace-123",
        "contentType": "image/png",
        "contentLength": 1024,
    }
    session, _ = _mock_s3_client(head_returns_none=False, meta_body=existing_meta)
    with patch("llogr.routes.media._s3_session", return_value=session):
        resp = client.get(
            f"/api/public/media/{MEDIA_ID}",
            headers=auth_headers,
        )

    assert resp.status_code == 200
    assert resp.json()["uploadedAt"] is None


def test_get_media_requires_auth(client):
    resp = client.get(f"/api/public/media/{MEDIA_ID}")
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Helper unit tests
# ---------------------------------------------------------------------------


def test_blob_key_without_prefix():
    cfg = S3Config(
        bucket="b", region="r", endpoint=None,
        access_key_id="a", secret_access_key="s",
    )
    assert _blob_key("tenant/user", "media123", cfg) == "tenant/user/media/media123.bin"


def test_blob_key_with_prefix():
    cfg = S3Config(
        bucket="b", region="r", endpoint=None,
        access_key_id="a", secret_access_key="s",
        key_prefix="data",
    )
    assert _blob_key("tenant/user", "media123", cfg) == "data/tenant/user/media/media123.bin"


def test_meta_key():
    cfg = S3Config(
        bucket="b", region="r", endpoint=None,
        access_key_id="a", secret_access_key="s",
    )
    assert _meta_key("pk", "m1", cfg) == "pk/media/m1.meta.json"
