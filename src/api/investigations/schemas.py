from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field


class InvestigationCreate(BaseModel):
    context_note: str | None = Field(default=None, max_length=2000)


class ExecutionBudget(BaseModel):
    max_steps: int = Field(default=20, ge=1, le=20)
    max_tool_calls: int = Field(default=40, ge=1, le=40)
    timeout_seconds: int = Field(default=1200, ge=1, le=1200)


class InvestigationView(BaseModel):
    id: UUID
    incident_id: UUID
    project_id: UUID
    environment_id: UUID
    status: str
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    attempt: int
    parent_investigation_id: UUID | None
    current_step: int
    tool_calls_count: int
    budget: ExecutionBudget
    failure_code: str | None
    failure_message: str | None
    cancel_requested: bool


class InvestigationsPage(BaseModel):
    items: list[InvestigationView]
    next_cursor: str | None = None


class CancelAck(BaseModel):
    investigation_id: UUID
    status: str
