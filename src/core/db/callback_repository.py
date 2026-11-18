from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.db.models import (
    Evidence,
    Hypothesis,
    InvestigationEvent,
    InvestigationResult,
    RcaReport,
    TimelineEntry,
)


class CallbackRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def event_by_id(self, event_id: UUID) -> InvestigationEvent | None:
        return await self.session.scalar(
            select(InvestigationEvent).where(InvestigationEvent.event_id == event_id)
        )

    async def event_by_sequence(
        self, investigation_id: UUID, sequence: int
    ) -> InvestigationEvent | None:
        return await self.session.get(InvestigationEvent, (investigation_id, sequence))

    async def result(self, investigation_id: UUID) -> InvestigationResult | None:
        return await self.session.get(InvestigationResult, investigation_id)

    async def hypotheses(self, investigation_id: UUID) -> list[Hypothesis]:
        return list(
            await self.session.scalars(
                select(Hypothesis)
                .where(Hypothesis.investigation_id == investigation_id)
                .order_by(Hypothesis.slot)
            )
        )

    async def evidence(
        self, investigation_id: UUID, *, after: UUID | None = None, limit: int = 51
    ) -> list[Evidence]:
        statement = select(Evidence).where(Evidence.investigation_id == investigation_id)
        if after is not None:
            statement = statement.where(Evidence.id > after)
        return list(await self.session.scalars(statement.order_by(Evidence.id).limit(limit)))

    async def evidence_by_id(self, evidence_id: UUID) -> Evidence | None:
        return await self.session.get(Evidence, evidence_id)

    async def events(
        self, investigation_id: UUID, after_sequence: int, limit: int
    ) -> list[InvestigationEvent]:
        return list(
            await self.session.scalars(
                select(InvestigationEvent)
                .where(
                    InvestigationEvent.investigation_id == investigation_id,
                    InvestigationEvent.sequence > after_sequence,
                )
                .order_by(InvestigationEvent.sequence)
                .limit(limit)
            )
        )

    async def timeline_entries(self, incident_id: UUID) -> list[TimelineEntry]:
        return list(
            await self.session.scalars(
                select(TimelineEntry)
                .where(TimelineEntry.incident_id == incident_id)
                .order_by(TimelineEntry.occurred_at)
            )
        )

    async def report(self, investigation_id: UUID) -> RcaReport | None:
        return await self.session.get(RcaReport, investigation_id)
