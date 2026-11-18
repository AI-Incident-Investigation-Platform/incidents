from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from src.api.incidents.schemas import IncidentView


class ContextService(BaseModel):
    id: UUID
    project_id: UUID
    environment_id: UUID
    name: str
    external_names: list[str]
    created_at: AwareDatetime


class ContextDeployment(BaseModel):
    id: UUID
    project_id: UUID
    environment_id: UUID
    service_id: UUID
    version: str
    deployed_at: AwareDatetime
    status: Literal["started", "succeeded", "failed", "rolled_back"]
    source: str


class ContextWindow(BaseModel):
    start: AwareDatetime = Field(serialization_alias="from")
    end: AwareDatetime = Field(serialization_alias="to")


class ContextIncident(IncidentView):
    version: int = Field(exclude=True)


class IncidentContext(BaseModel):
    incident: ContextIncident
    services: list[ContextService]
    deployments: list[ContextDeployment]
    window: ContextWindow


class InvestigationEventIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1.0"]
    event_id: UUID
    investigation_id: UUID
    incident_id: UUID
    sequence: int = Field(ge=1)
    type: str
    occurred_at: AwareDatetime
    data: dict


class AIResultIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1.0"]
    event_id: UUID
    investigation_id: UUID
    incident_id: UUID
    status: Literal["completed", "inconclusive", "failed", "timed_out", "canceled"]
    finished_at: AwareDatetime
    report: dict | None
    hypotheses: list[dict] = Field(max_length=3)
    evidence: list[dict]
    timeline: list[dict]
    error: str | None = None


class CallbackAck(BaseModel):
    accepted: bool
    deduplicated: bool
