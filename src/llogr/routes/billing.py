"""Billing summary endpoint and dashboard page — spend aggregated from ClickHouse."""
from __future__ import annotations

import csv
import io
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from jinja2 import Environment, FileSystemLoader

from llogr.auth import AuthContext, get_auth
from llogr.config import Settings, get_settings

logger = structlog.get_logger(__name__)
router = APIRouter()

_TEMPLATE_DIR = Path(__file__).resolve().parent.parent / "templates"
_jinja_env = Environment(loader=FileSystemLoader(str(_TEMPLATE_DIR)), autoescape=False)


def _period_bounds(period: str) -> tuple[datetime, datetime]:
    """Parse YYYY-MM → (start_inclusive, end_exclusive) in UTC."""
    try:
        year, month = int(period[:4]), int(period[5:7])
        if not (1 <= month <= 12):
            raise ValueError
    except (ValueError, IndexError):
        raise HTTPException(status_code=400, detail="Invalid period format. Use YYYY-MM.")
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    end = datetime(year + (month == 12), month % 12 + 1, 1, tzinfo=timezone.utc)
    return start, end


def _resolve_range(
    period: str,
    start: Optional[str],
    end: Optional[str],
) -> tuple[datetime, datetime, str]:
    """Resolve query params to (period_start, period_end, period_label).

    Priority:
      1. period=YYYY-MM   — calendar month boundaries
      2. start + end      — explicit ISO 8601 range (both required together)
      3. neither          — current month
    """
    if period and (start or end):
        raise HTTPException(
            status_code=400,
            detail="Provide either 'period' or 'start'/'end', not both.",
        )

    if start or end:
        if not (start and end):
            raise HTTPException(status_code=400, detail="Both 'start' and 'end' are required for a custom range.")
        try:
            def _parse(s: str) -> datetime:
                dt = datetime.fromisoformat(s)
                return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)
            dt_start, dt_end = _parse(start), _parse(end)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid datetime format. Use ISO 8601 (e.g. 2026-06-01T00:00:00).")
        if dt_end <= dt_start:
            raise HTTPException(status_code=400, detail="'end' must be after 'start'.")
        label = f"{dt_start.strftime('%Y-%m-%dT%H:%M:%S')}Z/{dt_end.strftime('%Y-%m-%dT%H:%M:%S')}Z"
        return dt_start, dt_end, label

    if not period:
        period = datetime.now(timezone.utc).strftime("%Y-%m")

    dt_start, dt_end = _period_bounds(period)
    return dt_start, dt_end, period


@router.get("/api/public/billing/summary")
async def billing_summary(
    period: str = Query(
        default="",
        description="Calendar month in YYYY-MM format (e.g. 2026-06). Mutually exclusive with start/end.",
    ),
    start: Optional[str] = Query(
        default=None,
        description="Range start in ISO 8601 format (e.g. 2026-06-01T00:00:00). Requires 'end'.",
    ),
    end: Optional[str] = Query(
        default=None,
        description="Range end in ISO 8601 format (e.g. 2026-06-30T23:59:59). Requires 'start'.",
    ),
    user_limit: int = Query(default=500, ge=1, le=5000, description="Max users per page."),
    user_offset: int = Query(default=0, ge=0, description="Users page offset."),
    auth: AuthContext = Depends(get_auth),
    settings: Settings = Depends(get_settings),
):
    """Return spend aggregated from ClickHouse for a billing period or custom range.

    **Period selection (mutually exclusive):**
    - `period=YYYY-MM` — full calendar month
    - `start` + `end`  — arbitrary ISO 8601 range
    - neither          — defaults to current month

    **Visibility is role-scoped via X-Role header:**
    - `USER` (default): own org group total + own user spend
    - `ORG_ADMIN`: all groups in org + per-user breakdown
    - `SUPER_ADMIN`: all orgs + all users globally
    """
    if not settings.clickhouse.url:
        raise HTTPException(status_code=503, detail="ClickHouse not configured")

    period_start, period_end, period_label = _resolve_range(period, start, end)

    from llogr.clickhouse import get_billing_summary_ch
    data = await get_billing_summary_ch(
        project_id=auth.public_key,
        settings=settings,
        is_org_admin=auth.is_org_admin,
        is_super_admin=auth.is_super_admin,
        period_start=period_start,
        period_end=period_end,
        user_limit=user_limit,
        user_offset=user_offset,
    )

    logger.info(
        "billing_summary",
        project_id=auth.public_key,
        period=period_label,
        groups=len(data["groups"]),
        users=len(data["users"]),
    )

    return {"period": period_label, **data}


_CSV_EXPORT_USER_LIMIT = 100_000
_UNSAFE_FILENAME_RE = re.compile(r"[^A-Za-z0-9_.-]")


@router.get("/api/public/billing/export")
async def billing_export(
    period: str = Query(
        default="",
        description="Calendar month in YYYY-MM format (e.g. 2026-06). Mutually exclusive with start/end.",
    ),
    start: Optional[str] = Query(default=None, description="Range start in ISO 8601 format. Requires 'end'."),
    end: Optional[str] = Query(default=None, description="Range end in ISO 8601 format. Requires 'start'."),
    auth: AuthContext = Depends(get_auth),
    settings: Settings = Depends(get_settings),
):
    """Download spend for a billing period as CSV.

    Same scoping and period rules as `/api/public/billing/summary`, but returns
    every group and (for admins) every user in the period rather than a page.
    """
    if not settings.clickhouse.url:
        raise HTTPException(status_code=503, detail="ClickHouse not configured")

    period_start, period_end, period_label = _resolve_range(period, start, end)

    from llogr.clickhouse import get_billing_summary_ch
    data = await get_billing_summary_ch(
        project_id=auth.public_key,
        settings=settings,
        is_org_admin=auth.is_org_admin,
        is_super_admin=auth.is_super_admin,
        period_start=period_start,
        period_end=period_end,
        user_limit=_CSV_EXPORT_USER_LIMIT,
        user_offset=0,
    )

    if data.get("has_more"):
        logger.warning(
            "billing_export_truncated",
            project_id=auth.public_key,
            period=period_label,
            user_limit=_CSV_EXPORT_USER_LIMIT,
        )

    def _generate():
        buf = io.StringIO()
        writer = csv.writer(buf)

        def _flush() -> str:
            value = buf.getvalue()
            buf.seek(0)
            buf.truncate(0)
            return value

        writer.writerow(["scope", "id", "total_usd", "input_usd", "output_usd", "requests"])
        yield _flush()
        for g in data["groups"]:
            writer.writerow(["org", g["org"], g["group_spent"], g["input_spent"], g["output_spent"], g["request_count"]])
            yield _flush()
        for u in data["users"]:
            writer.writerow(["user", u["project_id"], u["user_spent"], u["input_spent"], u["output_spent"], u["request_count"]])
            yield _flush()

    logger.info(
        "billing_export",
        project_id=auth.public_key,
        period=period_label,
        groups=len(data["groups"]),
        users=len(data["users"]),
    )

    filename = f"billing-{_UNSAFE_FILENAME_RE.sub('_', period_label)}.csv"
    return StreamingResponse(
        _generate(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/api/public/billing/who")
async def billing_who(auth: AuthContext = Depends(get_auth)):
    """Return the caller's resolved identity — used by the billing dashboard."""
    org = auth.public_key.split("/")[0] if auth.public_key else ""
    role = (
        "SUPER_ADMIN" if auth.is_super_admin
        else "ORG_ADMIN" if auth.is_org_admin
        else "USER"
    )
    return {"role": role, "project_id": auth.public_key, "org": org}


@router.get("/billing", response_class=HTMLResponse)
async def billing_dashboard(request: Request):
    """Serve the billing dashboard page."""
    base = request.scope.get("root_path", "").rstrip("/")
    template = _jinja_env.get_template("billing.html")
    return HTMLResponse(content=template.render(base_path=base))
