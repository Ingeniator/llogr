from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, Header
from fastapi.responses import JSONResponse

from llogr.auth import AuthContext, get_auth
from llogr.models import IngestionBatch, IngestionError, IngestionResponse, IngestionSuccess
from llogr.processing import ingest

router = APIRouter()


@router.post("/api/public/ingestion", response_model=IngestionResponse, status_code=207)
async def ingest_endpoint(
    batch: IngestionBatch,
    auth: AuthContext = Depends(get_auth),
    x_session_id: str | None = Header(default=None),
    x_request_id: str = Header(default=""),
    x_agent_name: str = Header(default=""),
) -> JSONResponse:
    session_id = x_session_id or f"fb-{uuid.uuid4().hex[:12]}"
    failed = await ingest(batch.batch, auth, session_id=session_id, request_id=x_request_id, agent_name=x_agent_name)
    if failed:
        message = f"storage failed: {', '.join(failed)}"
        errors = [IngestionError(id=event.id, status=500, message=message) for event in batch.batch]
        return JSONResponse(
            status_code=500,
            content=IngestionResponse(successes=[], errors=errors).model_dump(),
        )
    successes = [IngestionSuccess(id=event.id, status=201) for event in batch.batch]
    return JSONResponse(
        status_code=207,
        content=IngestionResponse(successes=successes, errors=[]).model_dump(),
    )
