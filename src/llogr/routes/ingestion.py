from __future__ import annotations

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from llogr.auth import AuthContext, get_auth
from llogr.models import IngestionBatch, IngestionResponse, IngestionSuccess
from llogr.processing import process_events

router = APIRouter()


@router.post("/api/public/ingestion", response_model=IngestionResponse, status_code=207)
def ingest(batch: IngestionBatch, auth: AuthContext = Depends(get_auth)) -> JSONResponse:
    process_events(batch.batch, auth)
    successes = [IngestionSuccess(id=event.id, status=201) for event in batch.batch]
    return JSONResponse(
        status_code=207,
        content=IngestionResponse(successes=successes, errors=[]).model_dump(),
    )
