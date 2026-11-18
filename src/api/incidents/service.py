import base64
import binascii
import json
from asyncio import timeout
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid7

from aiip_kit.access.checker import AccessChecker
from aiip_kit.access.model import Principal
from aiip_kit.errors import DependencyUnavailableError, ProjectNotFoundError
from aiip_kit.messaging.message import Actor, MessageV1
from aiip_kit.messaging.topology import Destination, live_project
from aiip_kit.outbox.writer import OutboxWriter
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.api.incidents.rules import IncidentConflict, resolve_status, validate_patch
from src.api.incidents.schemas import (
    IncidentCreate,
    IncidentPage,
    IncidentPatch,
    IncidentStatusChange,
    IncidentView,
    Overview,
    ReviewCreate,
    ReviewPage,
    ReviewView,
    TimelinePage,
    TimelineView,
)
from src.core.db.models import Incident, ProjectTombstone, Review, outbox
from src.core.db.repository import IncidentRepository
from src.core.projects_topology import ProjectsTopology


class IncidentNotFound(Exception):
    pass


class StaleIncident(Exception):
    pass


async def incident_view(repo: IncidentRepository, incident: Incident) -> IncidentView:
    services = await repo.services(incident.id)
    return IncidentView.model_validate(
        {
            **{
                field: getattr(incident, field)
                for field in IncidentView.model_fields
                if field != "service_ids"
            },
            "service_ids": [service.service_id for service in services],
        }
    )


def _cursor(incident: Incident) -> str:
    data = [incident.detected_at.isoformat(), str(incident.id)]
    return base64.urlsafe_b64encode(json.dumps(data).encode()).decode().rstrip("=")


def _severity_cursor(incident: Incident) -> str:
    rank = {"critical": 4, "high": 3, "medium": 2, "low": 1}[incident.severity]
    data = [rank, incident.detected_at.isoformat(), str(incident.id)]
    return base64.urlsafe_b64encode(json.dumps(data).encode()).decode().rstrip("=")


def _read_severity_cursor(value: str | None) -> tuple[int, datetime, UUID] | None:
    if value is None:
        return None
    try:
        raw = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
        rank, timestamp, incident_id = json.loads(raw)
        if type(rank) is not int or rank not in {1, 2, 3, 4}:
            raise ValueError
        return rank, datetime.fromisoformat(timestamp), UUID(incident_id)
    except (ValueError, TypeError, UnicodeDecodeError, binascii.Error) as exc:
        raise ValueError("Неверный cursor") from exc


def _read_cursor(value: str | None) -> tuple[datetime, UUID] | None:
    if value is None:
        return None
    try:
        raw = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
        timestamp, incident_id = json.loads(raw)
        return datetime.fromisoformat(timestamp), UUID(incident_id)
    except (ValueError, TypeError, UnicodeDecodeError, binascii.Error) as exc:
        raise ValueError("Неверный cursor") from exc


def _timeline_cursor(entry: TimelineView) -> str:
    data = [entry.occurred_at.isoformat(), str(entry.id)]
    return base64.urlsafe_b64encode(json.dumps(data).encode()).decode().rstrip("=")


class IncidentService:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        access: AccessChecker,
        writer: OutboxWriter | None = None,
        topology: ProjectsTopology | None = None,
    ) -> None:
        self._sessions = sessions
        self._access = access
        self._writer = writer or OutboxWriter(outbox)
        self._topology = topology

    async def overview(
        self,
        principal: Principal,
        authorization: str,
        project_id: UUID,
        environment_id: UUID | None,
        start: datetime | None,
        end: datetime | None,
    ) -> Overview:
        await self._access.require(principal, project_id, "project.read")
        if start is not None and end is not None and start > end:
            raise ValueError("from должен быть раньше to")
        if self._topology is None:
            raise DependencyUnavailableError("projects")
        try:
            async with timeout(5):
                healthy, total = await self._topology.counts(
                    project_id, environment_id, authorization
                )
        except TimeoutError as exc:
            raise DependencyUnavailableError("projects") from exc
        now = datetime.now(UTC)
        async with self._sessions() as session:
            opened, critical, active, resolved = await IncidentRepository(session).overview_counts(
                project_id, environment_id, start, end, now - timedelta(days=1), now
            )
        return Overview(
            open_incidents=opened,
            critical_incidents=critical,
            active_investigations=active,
            resolved_last_24h=resolved,
            healthy_services=healthy,
            total_services=total,
            generated_at=now,
        )

    async def _project_id(self, incident_id: UUID) -> UUID:
        async with self._sessions() as session:
            incident = await IncidentRepository(session).get(incident_id)
            if incident is None:
                raise IncidentNotFound
            return incident.project_id

    async def _view(self, repo: IncidentRepository, incident: Incident) -> IncidentView:
        return await incident_view(repo, incident)

    async def _event(
        self,
        session: AsyncSession,
        incident: Incident,
        type: str,
        actor_id: UUID,
        lifecycle_version: int,
        data: dict,
    ) -> None:
        message = MessageV1.event(
            type,
            "incidents",
            data,
            project_id=incident.project_id,
            aggregate_type="review" if type == "review.created" else "incident",
            aggregate_id=UUID(data["review"]["id"]) if type == "review.created" else incident.id,
            aggregate_version=1 if type == "review.created" else incident.version,
            lifecycle_version=lifecycle_version,
            actor=Actor(kind="user", id=str(actor_id)),
        )
        await self._writer.add(session, message, Destination.event("incidents", type))
        if type in {"incident.created", "incident.updated"}:
            live_message = message.model_copy(
                update={"message_id": uuid7(), "causation_id": message.message_id}
            )
            await self._writer.add(
                session,
                live_message,
                Destination.live("incidents", live_project(incident.project_id)),
            )

    async def create(self, principal: Principal, body: IncidentCreate) -> IncidentView:
        grant = await self._access.require(principal, body.project_id, "incident.manage")
        async with self._sessions.begin() as session:
            repo = IncidentRepository(session)
            display_number = await repo.next_display_number(body.project_id)
            if await session.get(ProjectTombstone, body.project_id) is not None:
                raise ProjectNotFoundError(body.project_id)
            incident = Incident(
                project_id=body.project_id,
                environment_id=body.environment_id,
                display_number=display_number,
                title=body.title.strip(),
                description=body.description,
                severity=body.severity,
                status="open",
                source=body.source,
                detected_at=body.detected_at,
                version=1,
            )
            await repo.add(incident)
            await repo.replace_services(incident.id, body.service_ids)
            await repo.add_timeline(
                incident.id, "incident.created", "Инцидент создан", "Ручное создание"
            )
            await session.flush()
            view = await self._view(repo, incident)
            await self._event(
                session,
                incident,
                "incident.created",
                principal.user_id,
                grant.lifecycle_version,
                {"incident": view.model_dump(mode="json")},
            )
            return view

    async def get(self, principal: Principal, incident_id: UUID) -> IncidentView:
        await self._access.require(principal, await self._project_id(incident_id), "project.read")
        async with self._sessions() as session:
            repo = IncidentRepository(session)
            incident = await repo.get(incident_id)
            if incident is None:
                raise IncidentNotFound
            return await self._view(repo, incident)

    async def list(
        self,
        principal: Principal,
        project_id: UUID,
        *,
        limit: int = 50,
        cursor: str | None = None,
        status: str | None = None,
        environment_id: UUID | None = None,
        service_id: UUID | None = None,
        severity: str | None = None,
        search: str | None = None,
        from_time: datetime | None = None,
        to_time: datetime | None = None,
        sort: str = "detected_at_desc",
    ) -> IncidentPage:
        await self._access.require(principal, project_id, "project.read")
        if sort not in {
            "detected_at_desc",
            "detected_at_asc",
            "-detected_at",
            "detected_at",
            "severity",
        }:
            raise ValueError("Неверная сортировка")
        if from_time is not None and to_time is not None and from_time > to_time:
            raise ValueError("from должен быть раньше to")
        async with self._sessions() as session:
            repo = IncidentRepository(session)
            incidents = await repo.list_incidents(
                project_id,
                limit=limit + 1,
                before=_read_cursor(cursor) if sort != "severity" else None,
                status=status,
                environment_id=environment_id,
                service_id=service_id,
                severity=severity,
                search=search,
                from_time=from_time,
                to_time=to_time,
                ascending=sort in {"detected_at_asc", "detected_at"},
                severity_sort=sort == "severity",
                severity_before=_read_severity_cursor(cursor) if sort == "severity" else None,
            )
            has_more = len(incidents) > limit
            incidents = incidents[:limit]
            return IncidentPage(
                items=[await self._view(repo, incident) for incident in incidents],
                next_cursor=(
                    (
                        _severity_cursor(incidents[-1])
                        if sort == "severity"
                        else _cursor(incidents[-1])
                    )
                    if has_more
                    else None
                ),
            )

    async def patch(
        self,
        principal: Principal,
        incident_id: UUID,
        body: IncidentPatch,
        version: int,
    ) -> IncidentView:
        values = validate_patch(body.model_dump(exclude_unset=True))
        grant = await self._access.require(
            principal, await self._project_id(incident_id), "incident.manage"
        )
        async with self._sessions.begin() as session:
            repo = IncidentRepository(session)
            incident = await repo.get(incident_id, lock=True)
            if incident is None:
                raise IncidentNotFound
            if incident.version != version:
                raise StaleIncident
            for field, value in values.items():
                if field == "service_ids":
                    previous = [item.service_id for item in await repo.services(incident.id)]
                    if previous != value:
                        await repo.replace_services(incident.id, value)
                        await repo.add_change(
                            incident.id,
                            principal.user_id,
                            field,
                            [str(x) for x in previous],
                            [str(x) for x in value],
                        )
                else:
                    previous = getattr(incident, field)
                    if previous != value:
                        setattr(incident, field, value)
                        await repo.add_change(
                            incident.id, principal.user_id, field, previous, value
                        )
            incident.version += 1
            incident.updated_at = datetime.now(UTC)
            await repo.add_timeline(
                incident.id, "incident.updated", "Инцидент изменён", ", ".join(values)
            )
            await session.flush()
            view = await self._view(repo, incident)
            await self._event(
                session,
                incident,
                "incident.updated",
                principal.user_id,
                grant.lifecycle_version,
                {"incident": view.model_dump(mode="json"), "changed": list(values)},
            )
            return view

    async def change_status(
        self,
        principal: Principal,
        incident_id: UUID,
        body: IncidentStatusChange,
    ) -> IncidentView:
        grant = await self._access.require(
            principal, await self._project_id(incident_id), "incident.manage"
        )
        async with self._sessions.begin() as session:
            repo = IncidentRepository(session)
            incident = await repo.get(incident_id, lock=True)
            if incident is None:
                raise IncidentNotFound
            target = resolve_status(
                incident.status,
                body.status,
                active_investigation=await repo.active_investigation(incident.id),
            )
            changed = target != incident.status
            if changed:
                previous = incident.status
                incident.status = target
                incident.resolved_at = datetime.now(UTC) if target == "resolved" else None
                incident.updated_at = datetime.now(UTC)
                incident.version += 1
                await repo.add_change(incident.id, principal.user_id, "status", previous, target)
                await repo.add_timeline(
                    incident.id,
                    "incident.resolved" if target == "resolved" else "incident.reopened",
                    "Инцидент закрыт" if target == "resolved" else "Инцидент открыт повторно",
                    body.comment or "",
                )
            await session.flush()
            view = await self._view(repo, incident)
            if changed:
                event_type = "incident.resolved" if target == "resolved" else "incident.updated"
                data = (
                    {
                        "incident_id": str(incident.id),
                        "resolved_by": str(principal.user_id),
                        "resolved_at": incident.resolved_at.isoformat(),
                    }
                    if target == "resolved" and incident.resolved_at is not None
                    else {"incident": view.model_dump(mode="json"), "changed": ["status"]}
                )
                await self._event(
                    session, incident, event_type, principal.user_id, grant.lifecycle_version, data
                )
                if target == "resolved":
                    live = MessageV1.event(
                        "incident.updated",
                        "incidents",
                        {"incident": view.model_dump(mode="json"), "changed": ["status"]},
                        project_id=incident.project_id,
                        aggregate_type="incident",
                        aggregate_id=incident.id,
                        aggregate_version=incident.version,
                        lifecycle_version=grant.lifecycle_version,
                        actor=Actor(kind="user", id=str(principal.user_id)),
                    )
                    await self._writer.add(
                        session,
                        live,
                        Destination.live("incidents", live_project(incident.project_id)),
                    )
            return view

    async def timeline(
        self,
        principal: Principal,
        incident_id: UUID,
        *,
        limit: int = 50,
        cursor: str | None = None,
    ) -> TimelinePage:
        await self._access.require(principal, await self._project_id(incident_id), "project.read")
        async with self._sessions() as session:
            repo = IncidentRepository(session)
            incident = await repo.get(incident_id)
            if incident is None:
                raise IncidentNotFound
            rows = await repo.timeline(incident_id, limit + 1, _read_cursor(cursor))
            has_more = len(rows) > limit
            items = [TimelineView.model_validate(item) for item in rows[:limit]]
            return TimelinePage(
                items=items,
                next_cursor=_timeline_cursor(items[-1]) if has_more else None,
            )

    async def add_review(
        self,
        principal: Principal,
        incident_id: UUID,
        body: ReviewCreate,
    ) -> ReviewView:
        grant = await self._access.require(
            principal, await self._project_id(incident_id), "review.write"
        )
        async with self._sessions.begin() as session:
            repo = IncidentRepository(session)
            incident = await repo.get(incident_id, lock=True)
            if incident is None:
                raise IncidentNotFound
            investigation = await repo.investigation(body.investigation_id)
            if investigation is None or investigation.incident_id != incident_id:
                raise IncidentNotFound
            if investigation.status not in {
                "completed",
                "inconclusive",
                "failed",
                "timed_out",
                "canceled",
            }:
                raise IncidentConflict("Review возможен после окончания расследования")
            if body.decision == "corrected" and not body.corrected_root_cause:
                raise ValueError("Для исправления нужна причина")
            review = Review(
                incident_id=incident_id,
                investigation_id=body.investigation_id,
                reviewer_id=principal.user_id,
                decision=body.decision,
                corrected_root_cause=body.corrected_root_cause,
                comment=body.comment,
            )
            session.add(review)
            await session.flush()
            await repo.add_timeline(
                incident_id,
                "review.created",
                "Review добавлен",
                body.comment,
                investigation_id=body.investigation_id,
            )
            await session.flush()
            view = ReviewView.model_validate(review)
            await self._event(
                session,
                incident,
                "review.created",
                principal.user_id,
                grant.lifecycle_version,
                {"review": view.model_dump(mode="json")},
            )
            return view

    async def reviews(
        self,
        principal: Principal,
        incident_id: UUID,
        *,
        limit: int = 50,
        cursor: str | None = None,
    ) -> ReviewPage:
        await self._access.require(principal, await self._project_id(incident_id), "report.read")
        async with self._sessions() as session:
            repo = IncidentRepository(session)
            incident = await repo.get(incident_id)
            if incident is None:
                raise IncidentNotFound
            rows = await repo.reviews(incident_id, limit + 1, _read_cursor(cursor))
            has_more = len(rows) > limit
            items = [ReviewView.model_validate(item) for item in rows[:limit]]
            next_cursor = None
            if has_more:
                data = [items[-1].created_at.isoformat(), str(items[-1].id)]
                next_cursor = (
                    base64.urlsafe_b64encode(json.dumps(data).encode()).decode().rstrip("=")
                )
            return ReviewPage(items=items, next_cursor=next_cursor)
