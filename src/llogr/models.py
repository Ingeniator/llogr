from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel

EventType = Literal[
    "trace-create",
    "span-create",
    "span-update",
    "generation-create",
    "generation-update",
    "event-create",
    "score-create",
    "sdk-log",
]


class IngestionEvent(BaseModel):
    id: str
    timestamp: str
    type: EventType
    body: dict[str, Any]


class IngestionBatch(BaseModel):
    batch: list[IngestionEvent]
    metadata: dict[str, Any] | None = None


class IngestionSuccess(BaseModel):
    id: str
    status: int


class IngestionError(BaseModel):
    id: str
    status: int
    message: str
    error: dict[str, Any] | None = None


class IngestionResponse(BaseModel):
    successes: list[IngestionSuccess]
    errors: list[IngestionError]
