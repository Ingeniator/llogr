"""Sessions list and per-session trace drill-down."""
from __future__ import annotations

from datetime import datetime, timezone

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query

from llogr.auth import AuthContext, get_auth
from llogr.config import Settings, get_settings

logger = structlog.get_logger(__name__)
router = APIRouter()


@router.get("/api/public/sessions")
async def list_sessions(
    start: datetime = Query(..., description="Window start (ISO 8601)"),
    end: datetime = Query(..., description="Window end (ISO 8601)"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    auth: AuthContext = Depends(get_auth),
    settings: Settings = Depends(get_settings),
):
    if not settings.clickhouse.url:
        raise HTTPException(status_code=503, detail="ClickHouse not configured")

    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    if end <= start:
        raise HTTPException(status_code=400, detail="end must be after start")

    from llogr.clickhouse import list_sessions_ch
    return await list_sessions_ch(
        project_id=auth.public_key,
        settings=settings,
        start=start,
        end=end,
        is_org_admin=auth.is_org_admin,
        is_super_admin=auth.is_super_admin and settings.features.superadmin_access,
        limit=limit,
        offset=offset,
    )


@router.get("/api/public/sessions/{session_id}")
async def get_session_traces(
    session_id: str,
    auth: AuthContext = Depends(get_auth),
    settings: Settings = Depends(get_settings),
):
    if not settings.clickhouse.url:
        raise HTTPException(status_code=503, detail="ClickHouse not configured")

    from llogr.clickhouse import get_session_traces_ch
    traces = await get_session_traces_ch(
        project_id=auth.public_key,
        settings=settings,
        session_id=session_id,
        is_org_admin=auth.is_org_admin,
        is_super_admin=auth.is_super_admin and settings.features.superadmin_access,
    )
    return {"session_id": session_id, "traces": traces}
