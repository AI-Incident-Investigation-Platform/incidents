from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.db.models import IdempotencyKey, Investigation


class InvestigationRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get(self, investigation_id: UUID, *, lock: bool = False) -> Investigation | None:
        statement = select(Investigation).where(Investigation.id == investigation_id)
        if lock:
            statement = statement.with_for_update()
        return await self.session.scalar(statement)

    async def history(self, incident_id: UUID) -> list[Investigation]:
        return list(
            await self.session.scalars(
                select(Investigation)
                .where(Investigation.incident_id == incident_id)
                .order_by(Investigation.attempt.desc())
            )
        )

    async def list_for_project(
        self,
        project_id: UUID,
        *,
        limit: int,
        status: str | None = None,
        before: tuple[datetime, UUID] | None = None,
    ) -> list[Investigation]:
        statement = select(Investigation).where(Investigation.project_id == project_id)
        if status is not None:
            statement = statement.where(Investigation.status == status)
        if before is not None:
            statement = statement.where(
                (Investigation.created_at < before[0])
                | ((Investigation.created_at == before[0]) & (Investigation.id < before[1]))
            )
        return list(
            await self.session.scalars(
                statement.order_by(Investigation.created_at.desc(), Investigation.id.desc()).limit(
                    limit
                )
            )
        )

    async def reserve_key(
        self, project_id: UUID, actor_id: UUID, operation: str, key: str, request_hash: str
    ) -> tuple[IdempotencyKey, bool]:
        values = {
            "project_id": project_id,
            "actor_id": actor_id,
            "operation": operation,
            "key": key,
            "request_hash": request_hash,
            "expires_at": datetime.now(UTC) + timedelta(hours=24),
        }
        inserted = await self.session.scalar(
            insert(IdempotencyKey)
            .values(**values)
            .on_conflict_do_nothing()
            .returning(IdempotencyKey.key)
        )
        row = await self.session.get(
            IdempotencyKey,
            (project_id, actor_id, operation, key),
        )
        assert row is not None
        return row, inserted is not None
