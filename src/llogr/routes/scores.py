from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Literal, Optional

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from llogr.auth import AuthContext, get_auth
from llogr.models import IngestionEvent
from llogr.processing import ingest

router = APIRouter()


class ScoreRequest(BaseModel):
    traceId: str
    name: str
    value: Any  # float for NUMERIC/BOOLEAN, str for CATEGORICAL/TEXT
    dataType: Optional[Literal["NUMERIC", "BOOLEAN", "CATEGORICAL", "TEXT"]] = None
    comment: Optional[str] = None
    observationId: Optional[str] = None
    configId: Optional[str] = None
    id: Optional[str] = None  # auto-generated if absent


class ScoreResponse(BaseModel):
    id: str


@router.post("/api/public/scores", response_model=ScoreResponse, status_code=201)
async def create_score(
    body: ScoreRequest,
    auth: AuthContext = Depends(get_auth),
) -> JSONResponse:
    """REST shim: accept a single score and funnel it through the ingestion pipeline.

    Converts to a score-create ingestion event so the score flows through llogr's
    storage (S3, ClickHouse) and forwarding (→ Langfuse) exactly as SDK-submitted scores do.
    """
    score_id = body.id or str(uuid.uuid4())

    score_body: dict[str, Any] = {
        "id": score_id,
        "traceId": body.traceId,
        "name": body.name,
        "value": body.value,
    }
    if body.dataType is not None:
        score_body["dataType"] = body.dataType
    if body.comment is not None:
        score_body["comment"] = body.comment
    if body.observationId is not None:
        score_body["observationId"] = body.observationId
    if body.configId is not None:
        score_body["configId"] = body.configId

    event = IngestionEvent(
        id=score_id,
        timestamp=datetime.now(timezone.utc).isoformat(),
        type="score-create",
        body=score_body,
    )

    failed = await ingest([event], auth)
    if failed:
        return JSONResponse(
            status_code=500,
            content={"error": f"storage failed: {', '.join(failed)}"},
        )

    return JSONResponse(
        status_code=201,
        content=ScoreResponse(id=score_id).model_dump(),
    )
