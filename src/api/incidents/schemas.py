from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

Severity = Literal["critical", "high", "medium", "low"]


class IncidentCreate(BaseModel):
    title: str = Field(min_length=1, max_length=300)
    description: str | None = None
    project_id: UUID
    environment_id: UUID
    service_ids: list[UUID] = Field(default_factory=list)
    severity: Severity
    detected_at: datetime
    source: str = "manual"


class IncidentPatch(BaseModel):
    title: str | None = Field(default=None, min_length=1, max_length=300)
    description: str | None = None
    severity: Severity | None = None
    service_ids: list[UUID] | None = None


class IncidentStatusChange(BaseModel):
    status: Literal["open", "resolved"]
    comment: str | None = None


class IncidentView(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    title: str
    description: str | None
    project_id: UUID
    environment_id: UUID
    service_ids: list[UUID]
    severity: Severity
    status: str
    source: str
    detected_at: datetime
    created_at: datetime
    updated_at: datetime
    resolved_at: datetime | None
    version: int


class IncidentPage(BaseModel):
    items: list[IncidentView]
    next_cursor: str | None = None


class Overview(BaseModel):
    open_incidents: int
    critical_incidents: int
    active_investigations: int
    resolved_last_24h: int
    healthy_services: int
    total_services: int
    generated_at: datetime


class ReviewCreate(BaseModel):
    investigation_id: UUID
    decision: Literal["confirmed", "rejected", "corrected"]
    corrected_root_cause: str | None = None
    comment: str = Field(min_length=1, max_length=4000)


class ReviewView(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    incident_id: UUID
    investigation_id: UUID
    reviewer_id: UUID
    decision: str
    corrected_root_cause: str | None
    comment: str
    created_at: datetime


class ReviewPage(BaseModel):
    items: list[ReviewView]
    next_cursor: str | None = None


class TimelineView(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    incident_id: UUID
    investigation_id: UUID | None
    event_type: str
    title: str
    description: str
    service_ids: list[UUID]
    source: dict
    causality: str
    occurred_at: datetime


class TimelinePage(BaseModel):
    items: list[TimelineView]
    next_cursor: str | None = None
