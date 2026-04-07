from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Optional

import aioboto3
import structlog
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from llogr.auth import AuthContext, get_auth
from llogr.config import Settings, get_settings
from llogr.s3 import _s3_client_config

logger = structlog.get_logger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class GetUploadUrlRequest(BaseModel):
    traceId: str
    observationId: Optional[str] = None
    contentType: str
    contentLength: int
    sha256Hash: str
    field: str


class GetUploadUrlResponse(BaseModel):
    uploadUrl: Optional[str] = None
    mediaId: str


class PatchMediaRequest(BaseModel):
    uploadedAt: str
    uploadHttpStatus: int
    uploadHttpError: Optional[str] = None
    uploadTimeMs: Optional[int] = None


class GetMediaResponse(BaseModel):
    mediaId: str
    contentType: str
    contentLength: int
    uploadedAt: Optional[datetime] = None
    url: str
    urlExpiry: str


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _media_key(public_key: str, media_id: str, s3_cfg) -> str:
    prefix = f"{public_key}/media/{media_id}"
    if s3_cfg.key_prefix:
        prefix = f"{s3_cfg.key_prefix.strip('/')}/{prefix}"
    return prefix


def _meta_key(public_key: str, media_id: str, s3_cfg) -> str:
    return _media_key(public_key, media_id, s3_cfg) + ".meta.json"


def _blob_key(public_key: str, media_id: str, s3_cfg) -> str:
    return _media_key(public_key, media_id, s3_cfg) + ".bin"


def _s3_session(s3_cfg):
    return aioboto3.Session(
        aws_access_key_id=s3_cfg.access_key_id,
        aws_secret_access_key=s3_cfg.secret_access_key,
        region_name=s3_cfg.region,
    )


async def _head_object(client, bucket: str, key: str) -> dict | None:
    try:
        return await client.head_object(Bucket=bucket, Key=key)
    except client.exceptions.ClientError as e:
        if int(e.response["Error"].get("Code", 0)) == 404:
            return None
        raise


# ---------------------------------------------------------------------------
# POST /api/public/media  — request a presigned upload URL
# ---------------------------------------------------------------------------

@router.post("/api/public/media", response_model=GetUploadUrlResponse)
async def get_upload_url(
    body: GetUploadUrlRequest,
    auth: AuthContext = Depends(get_auth),
    settings: Settings = Depends(get_settings),
) -> GetUploadUrlResponse:
    s3_cfg = settings.s3
    media_id = body.sha256Hash[:22]
    blob = _blob_key(auth.public_key, media_id, s3_cfg)
    meta = _meta_key(auth.public_key, media_id, s3_cfg)

    session = _s3_session(s3_cfg)
    async with session.client("s3", endpoint_url=s3_cfg.endpoint, config=_s3_client_config(s3_cfg)) as client:
        # Deduplication: if blob already exists, skip upload
        existing = await _head_object(client, s3_cfg.bucket, blob)
        if existing is not None:
            logger.debug("media_already_exists", media_id=media_id)
            return GetUploadUrlResponse(uploadUrl=None, mediaId=media_id)

        # Store metadata so we can serve GET later
        meta_body = {
            "mediaId": media_id,
            "traceId": body.traceId,
            "observationId": body.observationId,
            "contentType": body.contentType,
            "contentLength": body.contentLength,
            "sha256Hash": body.sha256Hash,
            "field": body.field,
            "createdAt": datetime.now(timezone.utc).isoformat(),
        }
        await client.put_object(
            Bucket=s3_cfg.bucket,
            Key=meta,
            Body=json.dumps(meta_body).encode(),
            ContentType="application/json",
        )

        # Generate presigned PUT URL for the SDK to upload directly
        upload_url = await client.generate_presigned_url(
            "put_object",
            Params={
                "Bucket": s3_cfg.bucket,
                "Key": blob,
                "ContentType": body.contentType,
            },
            ExpiresIn=s3_cfg.presign_expiry,
        )
        if s3_cfg.public_endpoint and s3_cfg.endpoint:
            upload_url = upload_url.replace(s3_cfg.endpoint, s3_cfg.public_endpoint, 1)

    logger.info("media_upload_url_created", media_id=media_id, trace_id=body.traceId)
    return GetUploadUrlResponse(uploadUrl=upload_url, mediaId=media_id)


# ---------------------------------------------------------------------------
# PATCH /api/public/media/{media_id}  — confirm upload status
# ---------------------------------------------------------------------------

@router.patch("/api/public/media/{media_id}", status_code=204)
async def patch_media(
    media_id: str,
    body: PatchMediaRequest,
    auth: AuthContext = Depends(get_auth),
    settings: Settings = Depends(get_settings),
) -> None:
    s3_cfg = settings.s3
    meta = _meta_key(auth.public_key, media_id, s3_cfg)

    session = _s3_session(s3_cfg)
    async with session.client("s3", endpoint_url=s3_cfg.endpoint, config=_s3_client_config(s3_cfg)) as client:
        existing = await _head_object(client, s3_cfg.bucket, meta)
        if existing is None:
            raise HTTPException(status_code=404, detail="Media not found")

        # Read existing metadata, merge upload status, write back
        obj = await client.get_object(Bucket=s3_cfg.bucket, Key=meta)
        meta_body = json.loads(await obj["Body"].read())
        meta_body["uploadedAt"] = body.uploadedAt
        meta_body["uploadHttpStatus"] = body.uploadHttpStatus
        meta_body["uploadHttpError"] = body.uploadHttpError
        meta_body["uploadTimeMs"] = body.uploadTimeMs

        await client.put_object(
            Bucket=s3_cfg.bucket,
            Key=meta,
            Body=json.dumps(meta_body, default=str).encode(),
            ContentType="application/json",
        )

    logger.info("media_upload_confirmed", media_id=media_id, status=body.uploadHttpStatus)


# ---------------------------------------------------------------------------
# GET /api/public/media/{media_id}  — retrieve media info + download URL
# ---------------------------------------------------------------------------

@router.get("/api/public/media/{media_id}", response_model=GetMediaResponse)
async def get_media(
    media_id: str,
    auth: AuthContext = Depends(get_auth),
    settings: Settings = Depends(get_settings),
) -> GetMediaResponse:
    s3_cfg = settings.s3
    meta = _meta_key(auth.public_key, media_id, s3_cfg)
    blob = _blob_key(auth.public_key, media_id, s3_cfg)

    session = _s3_session(s3_cfg)
    async with session.client("s3", endpoint_url=s3_cfg.endpoint, config=_s3_client_config(s3_cfg)) as client:
        existing = await _head_object(client, s3_cfg.bucket, meta)
        if existing is None:
            raise HTTPException(status_code=404, detail="Media not found")

        obj = await client.get_object(Bucket=s3_cfg.bucket, Key=meta)
        meta_body = json.loads(await obj["Body"].read())

        download_url = await client.generate_presigned_url(
            "get_object",
            Params={"Bucket": s3_cfg.bucket, "Key": blob},
            ExpiresIn=s3_cfg.presign_expiry,
        )
        if s3_cfg.public_endpoint and s3_cfg.endpoint:
            download_url = download_url.replace(s3_cfg.endpoint, s3_cfg.public_endpoint, 1)

    expiry = datetime.now(timezone.utc) + timedelta(seconds=s3_cfg.presign_expiry)

    uploaded_at_raw = meta_body.get("uploadedAt")
    uploaded_at = None
    if uploaded_at_raw:
        uploaded_at = datetime.fromisoformat(uploaded_at_raw)

    return GetMediaResponse(
        mediaId=media_id,
        contentType=meta_body["contentType"],
        contentLength=meta_body["contentLength"],
        uploadedAt=uploaded_at,
        url=download_url,
        urlExpiry=expiry.isoformat(),
    )
