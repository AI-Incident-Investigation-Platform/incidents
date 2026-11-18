from datetime import date, datetime
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.db.models import (
    Alert,
    CorrelationSlot,
    IngestReceipt,
    ProjectConfig,
    ProjectTombstone,
    QuotaCounter,
)


class AlertRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def receipt(self, receipt_id: UUID) -> IngestReceipt | None:
        return await self.session.get(IngestReceipt, receipt_id)

    async def tombstone(self, project_id: UUID) -> ProjectTombstone | None:
        return await self.session.get(ProjectTombstone, project_id)

    async def alert(self, project_id: UUID, source: str, source_event_id: str) -> Alert | None:
        return await self.session.scalar(
            select(Alert).where(
                Alert.project_id == project_id,
                Alert.source == source,
                Alert.source_event_id == source_event_id,
            )
        )

    async def incident_alerts(
        self,
        incident_id: UUID,
        limit: int,
        before: tuple[datetime, UUID] | None = None,
    ) -> list[Alert]:
        statement = select(Alert).where(Alert.incident_id == incident_id)
        if before is not None:
            statement = statement.where(
                (Alert.received_at < before[0])
                | ((Alert.received_at == before[0]) & (Alert.id < before[1]))
            )
        return list(
            await self.session.scalars(
                statement.order_by(Alert.received_at.desc(), Alert.id.desc()).limit(limit)
            )
        )

    async def slot(self, project_id: UUID, key: str) -> CorrelationSlot | None:
        return await self.session.scalar(
            select(CorrelationSlot)
            .where(CorrelationSlot.project_id == project_id, CorrelationSlot.correlation_key == key)
            .with_for_update()
        )

    async def latest_config(self, project_id: UUID) -> ProjectConfig | None:
        return await self.session.scalar(
            select(ProjectConfig)
            .where(ProjectConfig.project_id == project_id)
            .order_by(ProjectConfig.version.desc())
            .limit(1)
        )

    async def config_version(self, project_id: UUID, version: int) -> ProjectConfig | None:
        return await self.session.get(ProjectConfig, (project_id, version))

    async def consume_quota(
        self, project_id: UUID, day: date, limit: int, kind: str = "alerts"
    ) -> bool:
        if limit < 1:
            return False
        row = await self.session.scalar(
            insert(QuotaCounter)
            .values(project_id=project_id, day=day, kind=kind, used=1)
            .on_conflict_do_update(
                index_elements=["project_id", "day", "kind"],
                set_={"used": QuotaCounter.used + 1},
                where=QuotaCounter.used < limit,
            )
            .returning(QuotaCounter.used)
        )
        return row is not None

    async def lock_project(self, project_id: UUID) -> None:
        await self.session.execute(
            text("SELECT pg_advisory_xact_lock(hashtext(:project_id))"),
            {"project_id": f"incidents:{project_id}"},
        )
