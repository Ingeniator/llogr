from __future__ import annotations

from fastapi import APIRouter, Depends, Header
from fastapi.responses import JSONResponse

from llogr.auth import AuthContext, get_auth
from llogr.models import IngestionBatch, IngestionResponse, IngestionSuccess
from llogr.processing import ingest

router = APIRouter()


@router.post("/api/public/ingestion", response_model=IngestionResponse, status_code=207)
async def ingest_endpoint(
    batch: IngestionBatch,
    auth: AuthContext = Depends(get_auth),
    x_session_id: str = Header(default="none"),
) -> JSONResponse:
    await ingest(batch.batch, auth, session_id=x_session_id)
    successes = [IngestionSuccess(id=event.id, status=201) for event in batch.batch]
    return JSONResponse(
        status_code=207,
        content=IngestionResponse(successes=successes, errors=[]).model_dump(),
    )
