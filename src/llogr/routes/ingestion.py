from __future__ import annotations

from fastapi import APIRouter, BackgroundTasks, Depends, Header
from fastapi.responses import JSONResponse

from llogr.auth import AuthContext, get_auth
from llogr.models import IngestionBatch, IngestionResponse, IngestionSuccess
from llogr.processing import stage1_save_raw, stage2_forward

router = APIRouter()


@router.post("/api/public/ingestion", response_model=IngestionResponse, status_code=207)
async def ingest(
    batch: IngestionBatch,
    background_tasks: BackgroundTasks,
    auth: AuthContext = Depends(get_auth),
    x_session_id: str = Header(default="none"),
) -> JSONResponse:
    await stage1_save_raw(batch.batch, auth, session_id=x_session_id)
    background_tasks.add_task(stage2_forward, batch.batch, auth)
    successes = [IngestionSuccess(id=event.id, status=201) for event in batch.batch]
    return JSONResponse(
        status_code=207,
        content=IngestionResponse(successes=successes, errors=[]).model_dump(),
    )
