from datetime import datetime
from uuid import UUID

from sqlalchemy import Select, case, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.db.models import (
    Incident,
    IncidentChange,
    IncidentService,
    Investigation,
    Review,
    TimelineEntry,
)


class IncidentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def next_display_number(self, project_id: UUID) -> int:
        # Serialize number allocation for one project, including the first incident.
        await self.session.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:project_id))"),
            {"project_id": f"incidents:{project_id}"},
        )
        value = await self.session.scalar(
            select(func.max(Incident.display_number)).where(Incident.project_id == project_id)
        )
        return int(value or 0) + 1

    async def add(self, incident: Incident) -> None:
        self.session.add(incident)
        await self.session.flush()

    async def get(self, incident_id: UUID, *, lock: bool = False) -> Incident | None:
        statement: Select[tuple[Incident]] = select(Incident).where(Incident.id == incident_id)
        if lock:
            statement = statement.with_for_update()
        return await self.session.scalar(statement)

    async def services(self, incident_id: UUID) -> list[IncidentService]:
        result = await self.session.scalars(
            select(IncidentService).where(IncidentService.incident_id == incident_id)
        )
        return list(result)

    async def replace_services(self, incident_id: UUID, ids: list[UUID]) -> None:
        existing = {item.service_id: item for item in await self.services(incident_id)}
        for service_id, item in existing.items():
            if service_id not in ids:
                await self.session.delete(item)
        for service_id in ids:
            if service_id not in existing:
                self.session.add(
                    IncidentService(
                        incident_id=incident_id,
                        service_id=service_id,
                        name_snapshot=str(service_id),
                    )
                )

    async def active_investigation(self, incident_id: UUID) -> bool:
        value = await self.session.scalar(
            select(Investigation.id)
            .where(
                Investigation.incident_id == incident_id,
                Investigation.status.in_(("pending", "running")),
            )
            .limit(1)
        )
        return value is not None

    async def overview_counts(
        self,
        project_id: UUID,
        environment_id: UUID | None,
        start: datetime | None,
        end: datetime | None,
        resolved_since: datetime,
        now: datetime,
    ) -> tuple[int, int, int, int]:
        scope = [Incident.project_id == project_id]
        if environment_id is not None:
            scope.append(Incident.environment_id == environment_id)
        if start is not None:
            scope.append(Incident.detected_at >= start)
        if end is not None:
            scope.append(Incident.detected_at <= end)
        ongoing = Incident.status != "resolved"
        active = (
            select(func.count(Investigation.id))
            .join(Incident, Investigation.incident_id == Incident.id)
            .where(*scope, Investigation.status.in_(("pending", "running")))
            .correlate(None)
            .scalar_subquery()
        )
        result = await self.session.execute(
            select(
                func.count(Incident.id).filter(ongoing),
                func.count(Incident.id).filter(ongoing, Incident.severity == "critical"),
                active,
                func.count(Incident.id).filter(
                    Incident.resolved_at >= resolved_since, Incident.resolved_at <= now
                ),
            ).where(*scope)
        )
        opened, critical, running, resolved = result.one()
        return opened, critical, running, resolved

    async def investigation(self, investigation_id: UUID) -> Investigation | None:
        return await self.session.get(Investigation, investigation_id)

    async def add_change(
        self, incident_id: UUID, actor_id: UUID, field: str, old: object, new: object
    ) -> None:
        self.session.add(
            IncidentChange(
                incident_id=incident_id,
                actor_id=actor_id,
                field=field,
                old_value={"value": old},
                new_value={"value": new},
            )
        )

    async def add_timeline(
        self,
        incident_id: UUID,
        event_type: str,
        title: str,
        description: str,
        *,
        investigation_id: UUID | None = None,
        service_ids: list[str] | None = None,
    ) -> None:
        self.session.add(
            TimelineEntry(
                incident_id=incident_id,
                investigation_id=investigation_id,
                event_type=event_type,
                title=title,
                description=description,
                service_ids=service_ids or [],
                source={"source_type": "manual"},
                causality="observed",
            )
        )

    async def list_incidents(
        self,
        project_id: UUID,
        *,
        limit: int,
        before: tuple[datetime, UUID] | None = None,
        status: str | None = None,
        environment_id: UUID | None = None,
        service_id: UUID | None = None,
        severity: str | None = None,
        search: str | None = None,
        from_time: datetime | None = None,
        to_time: datetime | None = None,
        ascending: bool = False,
        severity_sort: bool = False,
        severity_before: tuple[int, datetime, UUID] | None = None,
    ) -> list[Incident]:
        statement = select(Incident).where(Incident.project_id == project_id)
        if status:
            statement = statement.where(Incident.status == status)
        if environment_id is not None:
            statement = statement.where(Incident.environment_id == environment_id)
        if service_id is not None:
            statement = statement.where(
                select(IncidentService.incident_id)
                .where(
                    IncidentService.incident_id == Incident.id,
                    IncidentService.service_id == service_id,
                )
                .exists()
            )
        if severity is not None:
            statement = statement.where(Incident.severity == severity)
        if search:
            statement = statement.where(
                Incident.title.icontains(search, autoescape=True)
                | Incident.description.icontains(search, autoescape=True)
            )
        if from_time is not None:
            statement = statement.where(Incident.detected_at >= from_time)
        if to_time is not None:
            statement = statement.where(Incident.detected_at <= to_time)
        if severity_sort:
            rank = case(
                (Incident.severity == "critical", 4),
                (Incident.severity == "high", 3),
                (Incident.severity == "medium", 2),
                (Incident.severity == "low", 1),
                else_=0,
            )
            if severity_before is not None:
                previous_rank, previous_time, previous_id = severity_before
                statement = statement.where(
                    (rank < previous_rank)
                    | ((rank == previous_rank) & (Incident.detected_at < previous_time))
                    | (
                        (rank == previous_rank)
                        & (Incident.detected_at == previous_time)
                        & (Incident.id < previous_id)
                    )
                )
            statement = statement.order_by(
                rank.desc(), Incident.detected_at.desc(), Incident.id.desc()
            ).limit(limit)
            return list(await self.session.scalars(statement))
        if before:
            if ascending:
                statement = statement.where(
                    (Incident.detected_at > before[0])
                    | ((Incident.detected_at == before[0]) & (Incident.id > before[1]))
                )
            else:
                statement = statement.where(
                    (Incident.detected_at < before[0])
                    | ((Incident.detected_at == before[0]) & (Incident.id < before[1]))
                )
        order = (
            (Incident.detected_at.asc(), Incident.id.asc())
            if ascending
            else (Incident.detected_at.desc(), Incident.id.desc())
        )
        statement = statement.order_by(*order).limit(limit)
        return list(await self.session.scalars(statement))

    async def timeline(
        self,
        incident_id: UUID,
        limit: int,
        before: tuple[datetime, UUID] | None = None,
    ) -> list[TimelineEntry]:
        statement = select(TimelineEntry).where(TimelineEntry.incident_id == incident_id)
        if before is not None:
            statement = statement.where(
                (TimelineEntry.occurred_at < before[0])
                | ((TimelineEntry.occurred_at == before[0]) & (TimelineEntry.id < before[1]))
            )
        return list(
            await self.session.scalars(
                statement.order_by(TimelineEntry.occurred_at.desc(), TimelineEntry.id.desc()).limit(
                    limit
                )
            )
        )

    async def reviews(
        self, incident_id: UUID, limit: int, before: tuple[datetime, UUID] | None = None
    ) -> list[Review]:
        statement = select(Review).where(Review.incident_id == incident_id)
        if before is not None:
            statement = statement.where(
                (Review.created_at < before[0])
                | ((Review.created_at == before[0]) & (Review.id < before[1]))
            )
        return list(
            await self.session.scalars(
                statement.order_by(Review.created_at.desc(), Review.id.desc()).limit(limit)
            )
        )
