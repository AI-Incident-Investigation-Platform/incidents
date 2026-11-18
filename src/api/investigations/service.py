import base64
import binascii
import hashlib
import json
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid7

from aiip_kit.access.checker import AccessChecker
from aiip_kit.access.model import Principal, ProjectGrant
from aiip_kit.messaging.message import Actor, MessageV1
from aiip_kit.messaging.topology import Destination, live_project
from aiip_kit.outbox.writer import OutboxWriter
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.api.incidents.service import IncidentNotFound, incident_view
from src.api.investigations.rules import InvestigationConflict, next_attempt, retry_allowed
from src.api.investigations.schemas import (
    CancelAck,
    ExecutionBudget,
    InvestigationCreate,
    InvestigationsPage,
    InvestigationView,
)
from src.core.db.investigation_repository import InvestigationRepository
from src.core.db.models import Incident, Investigation, outbox
from src.core.db.repository import IncidentRepository


class InvestigationNotFound(Exception):
    pass


class IdempotencyConflict(Exception):
    pass


def _request_hash(*values: object) -> str:
    canonical = json.dumps(values, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()


class InvestigationService:
    def __init__(self, sessions: async_sessionmaker[AsyncSession], access: AccessChecker) -> None:
        self._sessions = sessions
        self._access = access
        self._outbox = OutboxWriter(outbox)

    async def _incident_project(self, incident_id: UUID) -> UUID:
        async with self._sessions() as session:
            incident = await IncidentRepository(session).get(incident_id)
            if incident is None:
                raise IncidentNotFound
            return incident.project_id

    async def _investigation_project(self, investigation_id: UUID) -> UUID:
        async with self._sessions() as session:
            run = await InvestigationRepository(session).get(investigation_id)
            if run is None:
                raise InvestigationNotFound
            return run.project_id

    @staticmethod
    def _view(run: Investigation, incident: Incident) -> InvestigationView:
        return InvestigationView(
            id=run.id,
            incident_id=run.incident_id,
            project_id=run.project_id,
            environment_id=incident.environment_id,
            status=run.status,
            created_at=run.created_at,
            started_at=run.started_at,
            finished_at=run.finished_at,
            attempt=run.attempt,
            parent_investigation_id=run.parent_investigation_id,
            current_step=run.current_step,
            tool_calls_count=run.tool_calls_count,
            budget=ExecutionBudget.model_validate(run.budget),
            failure_code=run.failure_code,
            failure_message=run.failure_message,
            cancel_requested=run.cancel_requested_at is not None,
        )

    async def _event(
        self,
        session: AsyncSession,
        run: Investigation,
        incident: Incident,
        grant: ProjectGrant,
        actor_id: UUID,
    ) -> None:
        message = MessageV1.event(
            "investigation.created",
            "incidents",
            {"investigation": self._view(run, incident).model_dump(mode="json")},
            aggregate_type="investigation",
            aggregate_id=run.id,
            aggregate_version=1,
            project_id=run.project_id,
            lifecycle_version=grant.lifecycle_version,
            actor=Actor(kind="user", id=str(actor_id)),
        )
        await self._outbox.add(
            session, message, Destination.event("incidents", "investigation.created")
        )
        await self._publish_incident_updated(session, incident, grant, actor_id, message.message_id)

    async def _publish_incident_updated(
        self,
        session: AsyncSession,
        incident: Incident,
        grant: ProjectGrant,
        actor_id: UUID,
        causation_id: UUID,
    ) -> None:
        changed = MessageV1.event(
            "incident.updated",
            "incidents",
            {
                "incident": (await incident_view(IncidentRepository(session), incident)).model_dump(
                    mode="json"
                ),
                "changed": ["status"],
            },
            aggregate_type="incident",
            aggregate_id=incident.id,
            aggregate_version=incident.version,
            project_id=incident.project_id,
            lifecycle_version=grant.lifecycle_version,
            actor=Actor(kind="user", id=str(actor_id)),
            causation_id=causation_id,
        )
        await self._outbox.add(session, changed, Destination.event("incidents", "incident.updated"))
        await self._outbox.add(
            session,
            changed.model_copy(update={"message_id": uuid7(), "causation_id": changed.message_id}),
            Destination.live("incidents", live_project(incident.project_id)),
        )

    async def _create_locked(
        self,
        session: AsyncSession,
        incident: Incident,
        grant: ProjectGrant,
        actor_id: UUID,
        *,
        context_note: str | None,
        parent_id: UUID | None,
    ) -> Investigation:
        repo = InvestigationRepository(session)
        history = await repo.history(incident.id)
        attempt = next_attempt([(item.status, item.attempt) for item in history])
        budget = ExecutionBudget()
        now = datetime.now(UTC)
        run = Investigation(
            incident_id=incident.id,
            project_id=incident.project_id,
            attempt=attempt,
            status="pending",
            snapshot={
                "incident_id": str(incident.id),
                "version": incident.version,
                "title": incident.title,
                "severity": incident.severity,
                "lifecycle_version": grant.lifecycle_version,
            },
            budget=budget.model_dump(),
            context_note=context_note,
            admission_state="not_sent",
            created_at=now,
            deadline=now + timedelta(seconds=budget.timeout_seconds),
            parent_investigation_id=parent_id,
            current_step=0,
            tool_calls_count=0,
        )
        session.add(run)
        await session.flush()
        incident.status = "investigating"
        incident.updated_at = now
        incident.version += 1
        await IncidentRepository(session).add_timeline(
            incident.id,
            "investigation.created",
            "Расследование запущено",
            f"Попытка {attempt}",
            investigation_id=run.id,
        )
        await self._event(session, run, incident, grant, actor_id)
        return run

    async def start(
        self, principal: Principal, incident_id: UUID, body: InvestigationCreate, key: str
    ) -> InvestigationView:
        grant = await self._access.require(
            principal, await self._incident_project(incident_id), "investigation.run"
        )
        if not key or len(key) > 200:
            raise ValueError("Нужен Idempotency-Key длиной до 200 символов")
        digest = _request_hash(incident_id, body.model_dump())
        async with self._sessions.begin() as session:
            repo = InvestigationRepository(session)
            row, fresh = await repo.reserve_key(
                grant.project_id, principal.user_id, "start", key, digest
            )
            if row.request_hash != digest:
                raise IdempotencyConflict
            if not fresh:
                if row.response is None:
                    raise InvestigationConflict("Незавершённый повтор запроса")
                existing = await repo.get(UUID(row.response["investigation_id"]))
                assert existing is not None
                incident = await IncidentRepository(session).get(incident_id)
                assert incident is not None
                return self._view(existing, incident)
            incident = await IncidentRepository(session).get(incident_id, lock=True)
            if incident is None:
                raise IncidentNotFound
            if incident.status == "resolved":
                raise InvestigationConflict("Закрытый инцидент нужно сначала открыть")
            run = await self._create_locked(
                session,
                incident,
                grant,
                principal.user_id,
                context_note=body.context_note,
                parent_id=None,
            )
            row.response = {"investigation_id": str(run.id)}
            await session.flush()
            return self._view(run, incident)

    async def get(self, principal: Principal, investigation_id: UUID) -> InvestigationView:
        project_id = await self._investigation_project(investigation_id)
        await self._access.require(principal, project_id, "project.read")
        async with self._sessions() as session:
            repo = InvestigationRepository(session)
            run = await repo.get(investigation_id)
            if run is None:
                raise InvestigationNotFound
            incident = await IncidentRepository(session).get(run.incident_id)
            assert incident is not None
            return self._view(run, incident)

    async def list_for_incident(
        self, principal: Principal, incident_id: UUID
    ) -> InvestigationsPage:
        await self._access.require(
            principal, await self._incident_project(incident_id), "project.read"
        )
        async with self._sessions() as session:
            incident = await IncidentRepository(session).get(incident_id)
            assert incident is not None
            runs = await InvestigationRepository(session).history(incident_id)
            return InvestigationsPage(items=[self._view(run, incident) for run in runs])

    async def list_for_project(
        self,
        principal: Principal,
        project_id: UUID,
        *,
        status: str | None = None,
        cursor: str | None = None,
        limit: int = 50,
    ) -> InvestigationsPage:
        await self._access.require(principal, project_id, "project.read")
        before = None
        if cursor is not None:
            try:
                value = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4))
                timestamp, run_id = json.loads(value)
                before = (datetime.fromisoformat(timestamp), UUID(run_id))
            except (ValueError, TypeError, UnicodeDecodeError, binascii.Error) as exc:
                raise ValueError("Неверный cursor") from exc
        async with self._sessions() as session:
            repo = InvestigationRepository(session)
            runs = await repo.list_for_project(
                project_id, limit=limit + 1, status=status, before=before
            )
            has_more = len(runs) > limit
            runs = runs[:limit]
            incidents = {
                run.incident_id: await IncidentRepository(session).get(run.incident_id)
                for run in runs
            }
            items = []
            for run in runs:
                incident = incidents[run.incident_id]
                assert incident is not None
                items.append(self._view(run, incident))
            next_cursor = None
            if has_more and runs:
                data = json.dumps([runs[-1].created_at.isoformat(), str(runs[-1].id)])
                next_cursor = base64.urlsafe_b64encode(data.encode()).decode().rstrip("=")
            return InvestigationsPage(items=items, next_cursor=next_cursor)

    async def cancel(self, principal: Principal, investigation_id: UUID) -> CancelAck:
        project_id = await self._investigation_project(investigation_id)
        grant = await self._access.require(principal, project_id, "investigation.run")
        async with self._sessions.begin() as session:
            repo = InvestigationRepository(session)
            run = await repo.get(investigation_id)
            if run is None:
                raise InvestigationNotFound
            incident = await IncidentRepository(session).get(run.incident_id, lock=True)
            assert incident is not None
            run = await repo.get(investigation_id, lock=True)
            assert run is not None
            if run.status in {"pending", "running"} and run.cancel_requested_at is None:
                now = datetime.now(UTC)
                run.cancel_requested_at = now
                run.next_start_at = None
                if run.status == "pending" and run.admission_state == "not_sent":
                    run.status = "canceled"
                    run.finished_at = now
                    incident.status = "open"
                    incident.version += 1
                    finished = MessageV1.event(
                        "investigation.finished",
                        "incidents",
                        {
                            "investigation_id": str(run.id),
                            "incident_id": str(incident.id),
                            "status": "canceled",
                            "outcome": None,
                        },
                        project_id=run.project_id,
                        lifecycle_version=grant.lifecycle_version,
                        aggregate_type="investigation",
                        aggregate_id=run.id,
                        actor=Actor(kind="user", id=str(principal.user_id)),
                    )
                    await self._outbox.add(
                        session,
                        finished,
                        Destination.event("incidents", "investigation.finished"),
                    )
                    await self._publish_incident_updated(
                        session, incident, grant, principal.user_id, finished.message_id
                    )
                await session.flush()
            return CancelAck(investigation_id=run.id, status=run.status)

    async def retry(
        self, principal: Principal, investigation_id: UUID, key: str
    ) -> InvestigationView:
        project_id = await self._investigation_project(investigation_id)
        grant = await self._access.require(principal, project_id, "investigation.run")
        if not key or len(key) > 200:
            raise ValueError("Нужен Idempotency-Key длиной до 200 символов")
        digest = _request_hash(investigation_id)
        async with self._sessions.begin() as session:
            repo = InvestigationRepository(session)
            row, fresh = await repo.reserve_key(project_id, principal.user_id, "retry", key, digest)
            if row.request_hash != digest:
                raise IdempotencyConflict
            parent = await repo.get(investigation_id)
            if parent is None:
                raise InvestigationNotFound
            incident = await IncidentRepository(session).get(parent.incident_id, lock=True)
            assert incident is not None
            if not fresh:
                if row.response is None:
                    raise InvestigationConflict("Незавершённый повтор запроса")
                existing = await repo.get(UUID(row.response["investigation_id"]))
                assert existing is not None
                return self._view(existing, incident)
            if not retry_allowed(parent.status):
                raise InvestigationConflict("Повтор возможен после завершения попытки")
            if incident.status == "resolved":
                raise InvestigationConflict("Закрытый инцидент нужно сначала открыть")
            run = await self._create_locked(
                session,
                incident,
                grant,
                principal.user_id,
                context_note=parent.context_note,
                parent_id=parent.id,
            )
            row.response = {"investigation_id": str(run.id)}
            await session.flush()
            return self._view(run, incident)
