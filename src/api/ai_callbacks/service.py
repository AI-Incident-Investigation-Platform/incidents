from datetime import UTC, datetime, timedelta
from typing import Protocol
from uuid import UUID, uuid7

from aiip_kit.errors import DependencyUnavailableError
from aiip_kit.messaging.message import Actor, MessageV1
from aiip_kit.messaging.topology import Destination, live_investigation, live_project
from aiip_kit.outbox.writer import OutboxWriter
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.api.ai_callbacks.schemas import (
    AIResultIn,
    CallbackAck,
    ContextDeployment,
    ContextIncident,
    ContextService,
    ContextWindow,
    IncidentContext,
    InvestigationEventIn,
)
from src.api.incidents.service import IncidentNotFound, incident_view
from src.api.investigations.service import InvestigationNotFound
from src.core.db.callback_repository import CallbackRepository
from src.core.db.investigation_repository import InvestigationRepository
from src.core.db.models import (
    Evidence,
    Hypothesis,
    InvestigationEvent,
    InvestigationResult,
    RcaReport,
    TimelineEntry,
    outbox,
)
from src.core.db.repository import IncidentRepository


class CallbackConflict(Exception):
    pass


class ProjectContextSource(Protocol):
    async def fetch(
        self,
        project_id: UUID,
        environment_id: UUID,
        service_ids: list[UUID],
        start: datetime,
        end: datetime,
    ) -> tuple[list[ContextService], list[ContextDeployment]]: ...


class CallbackService:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        context_source: ProjectContextSource | None = None,
    ) -> None:
        self._sessions = sessions
        self._outbox = OutboxWriter(outbox)
        self._context_source = context_source

    async def context(self, incident_id: UUID) -> IncidentContext:
        async with self._sessions() as session:
            repo = IncidentRepository(session)
            row = await repo.get(incident_id)
            if row is None:
                raise IncidentNotFound
            incident = ContextIncident.model_validate((await incident_view(repo, row)).model_dump())
        window = ContextWindow(
            start=incident.detected_at - timedelta(minutes=30),
            end=incident.detected_at + timedelta(minutes=5),
        )
        if not incident.service_ids:
            return IncidentContext(incident=incident, services=[], deployments=[], window=window)
        if self._context_source is None:
            raise DependencyUnavailableError("projects")
        services, deployments = await self._context_source.fetch(
            incident.project_id,
            incident.environment_id,
            incident.service_ids,
            window.start,
            window.end,
        )
        service_ids = set(incident.service_ids)
        return IncidentContext(
            incident=incident,
            window=window,
            services=[
                service
                for service in services
                if service.project_id == incident.project_id
                and service.environment_id == incident.environment_id
                and service.id in service_ids
            ],
            deployments=[
                deployment
                for deployment in deployments
                if deployment.project_id == incident.project_id
                and deployment.environment_id == incident.environment_id
                and deployment.service_id in service_ids
                and window.start <= deployment.deployed_at <= window.end
            ],
        )

    async def _incident_id(self, investigation_id: UUID) -> UUID:
        async with self._sessions() as session:
            run = await InvestigationRepository(session).get(investigation_id)
            if run is None:
                raise InvestigationNotFound
            return run.incident_id

    async def event(self, body: InvestigationEventIn) -> CallbackAck:
        if await self._incident_id(body.investigation_id) != body.incident_id:
            raise CallbackConflict("Расследование не относится к инциденту")
        async with self._sessions.begin() as session:
            incident = await IncidentRepository(session).get(body.incident_id, lock=True)
            if incident is None:
                raise IncidentNotFound
            run = await InvestigationRepository(session).get(body.investigation_id, lock=True)
            if run is None:
                raise InvestigationNotFound
            repo = CallbackRepository(session)
            previous = await repo.event_by_id(body.event_id)
            if previous is not None:
                if (
                    previous.investigation_id != body.investigation_id
                    or previous.sequence != body.sequence
                ):
                    raise CallbackConflict("event_id уже использован")
                return CallbackAck(accepted=False, deduplicated=True)
            if await repo.event_by_sequence(body.investigation_id, body.sequence) is not None:
                raise CallbackConflict("sequence уже использован")
            if run.status in {"completed", "inconclusive", "failed", "timed_out", "canceled"}:
                return CallbackAck(accepted=False, deduplicated=False)
            session.add(
                InvestigationEvent(
                    investigation_id=body.investigation_id,
                    sequence=body.sequence,
                    event_id=body.event_id,
                    kind=body.type,
                    payload=body.model_dump(mode="json"),
                    occurred_at=body.occurred_at,
                )
            )
            if body.type == "investigation.started":
                run.status = "running"
                run.started_at = body.occurred_at
            if body.type.startswith("tool."):
                run.tool_calls_count += 1
            if isinstance(body.data.get("current_step"), int):
                run.current_step = max(run.current_step, body.data["current_step"])
            event = MessageV1.event(
                "investigation.event",
                "incidents",
                body.model_dump(mode="json"),
                project_id=run.project_id,
                lifecycle_version=run.snapshot.get("lifecycle_version"),
                aggregate_type="investigation",
                aggregate_id=run.id,
                aggregate_version=body.sequence,
                actor=Actor(kind="service", id="ai-backend"),
            )
            await self._outbox.add(
                session, event, Destination.event("incidents", "investigation.event")
            )
            await self._outbox.add(
                session,
                event.model_copy(update={"message_id": UUID(int=body.event_id.int ^ 1)}),
                Destination.live("incidents", live_investigation(run.project_id, run.id)),
            )
            return CallbackAck(accepted=True, deduplicated=False)

    async def result(self, body: AIResultIn) -> CallbackAck:
        if await self._incident_id(body.investigation_id) != body.incident_id:
            raise CallbackConflict("Расследование не относится к инциденту")
        async with self._sessions.begin() as session:
            incident_repo = IncidentRepository(session)
            incident = await incident_repo.get(body.incident_id, lock=True)
            if incident is None:
                raise IncidentNotFound
            run = await InvestigationRepository(session).get(body.investigation_id, lock=True)
            if run is None:
                raise InvestigationNotFound
            repo = CallbackRepository(session)
            if await repo.result(run.id) is not None:
                return CallbackAck(accepted=False, deduplicated=True)
            if run.status in {"completed", "inconclusive", "failed", "timed_out", "canceled"}:
                return CallbackAck(accepted=False, deduplicated=True)
            session.add(
                InvestigationResult(
                    investigation_id=run.id,
                    status=body.status,
                    payload=body.model_dump(mode="json"),
                )
            )
            run.status = body.status
            run.finished_at = body.finished_at
            if body.error:
                run.failure_message = body.error[:1000]
            if body.report is not None:
                session.add(
                    RcaReport(
                        investigation_id=run.id,
                        summary=str(body.report.get("summary", "")),
                        root_cause=body.report.get("root_cause"),
                        recommendations=body.report.get("remediation", []),
                    )
                )
            for slot, hypothesis in enumerate(body.hypotheses, start=1):
                session.add(
                    Hypothesis(
                        id=UUID(hypothesis["id"]),
                        investigation_id=run.id,
                        slot=slot,
                        title=hypothesis["title"],
                        explanation=hypothesis["explanation"],
                        status=hypothesis["status"],
                        confidence=hypothesis["confidence"],
                    )
                )
            for evidence in body.evidence:
                source = evidence.get("source", {})
                session.add(
                    Evidence(
                        id=UUID(evidence["id"]),
                        investigation_id=run.id,
                        hypothesis_id=None,
                        kind=source.get("source_type", "manual"),
                        title=evidence["title"],
                        data=evidence,
                    )
                )
            for entry in body.timeline:
                session.add(
                    TimelineEntry(
                        id=UUID(entry["id"]),
                        incident_id=incident.id,
                        investigation_id=run.id,
                        event_type=entry["event_type"],
                        title=entry["title"],
                        description=entry.get("description", ""),
                        service_ids=entry.get("service_ids", []),
                        source=entry.get("source", {"source_type": "manual"}),
                        causality=entry.get("causality", "unknown"),
                        occurred_at=datetime.fromisoformat(entry["occurred_at"]),
                    )
                )
            incident.status = (
                "awaiting_review" if body.status in {"completed", "inconclusive"} else "open"
            )
            incident.updated_at = datetime.now(UTC)
            incident.version += 1
            await incident_repo.add_timeline(
                incident.id,
                "investigation.finished",
                "Расследование завершено",
                body.status,
                investigation_id=run.id,
            )
            event = MessageV1.event(
                "investigation.finished",
                "incidents",
                {
                    "investigation_id": str(run.id),
                    "incident_id": str(incident.id),
                    "status": body.status,
                    "outcome": body.status
                    if body.status in {"completed", "inconclusive"}
                    else None,
                },
                project_id=run.project_id,
                lifecycle_version=run.snapshot.get("lifecycle_version"),
                aggregate_type="investigation",
                aggregate_id=run.id,
                actor=Actor(kind="service", id="ai-backend"),
            )
            await self._outbox.add(
                session, event, Destination.event("incidents", "investigation.finished")
            )
            changed = MessageV1.event(
                "incident.updated",
                "incidents",
                {
                    "incident": (await incident_view(incident_repo, incident)).model_dump(
                        mode="json"
                    ),
                    "changed": ["status"],
                },
                project_id=run.project_id,
                lifecycle_version=run.snapshot.get("lifecycle_version"),
                aggregate_type="incident",
                aggregate_id=incident.id,
                aggregate_version=incident.version,
                actor=Actor(kind="service", id="ai-backend"),
                causation_id=event.message_id,
            )
            await self._outbox.add(
                session, changed, Destination.event("incidents", "incident.updated")
            )
            await self._outbox.add(
                session,
                changed.model_copy(
                    update={"message_id": uuid7(), "causation_id": changed.message_id}
                ),
                Destination.live("incidents", live_project(run.project_id)),
            )
            return CallbackAck(accepted=True, deduplicated=False)
