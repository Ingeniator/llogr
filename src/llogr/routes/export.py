"""Streaming JSONL export of generation events."""
from __future__ import annotations

from datetime import datetime, timezone

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query
from starlette.responses import StreamingResponse

from llogr.auth import AuthContext, get_auth
from llogr.config import Settings, get_settings

logger = structlog.get_logger(__name__)

router = APIRouter()


@router.get("/api/public/export")
async def export_generations(
    start: datetime = Query(..., description="Export window start (ISO 8601)"),
    end: datetime = Query(..., description="Export window end (ISO 8601)"),
    session_id: str = Query(default="", description="Filter to a single session"),
    auth: AuthContext = Depends(get_auth),
    settings: Settings = Depends(get_settings),
):
    """Stream generation events as JSONL for the given time range.

    Auth scoping is automatic: regular users see their own group_id,
    org admins see all groups under their org prefix.
    """
    if not settings.clickhouse.url:
        raise HTTPException(status_code=503, detail="ClickHouse not configured")

    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    if end <= start:
        raise HTTPException(status_code=400, detail="end must be after start")

    from llogr.clickhouse import export_generations_ch

    logger.info(
        "export_requested",
        project_id=auth.public_key,
        start=start.isoformat(),
        end=end.isoformat(),
    )

    return StreamingResponse(
        export_generations_ch(
            project_id=auth.public_key,
            settings=settings,
            start=start,
            end=end,
            is_org_admin=auth.is_org_admin,
            is_super_admin=auth.is_super_admin and settings.features.superadmin_access,
            session_id=session_id or None,
        ),
        media_type="application/x-ndjson",
        headers={"Content-Disposition": 'attachment; filename="export.jsonl"'},
    )
