from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid7

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.db.models import Incident, IncidentService, Investigation


@dataclass(frozen=True)
class DispatchClaim:
    investigation_id: UUID
    lease_token: UUID
    admission_state: str
    request: dict


@dataclass(frozen=True)
class CancelClaim:
    investigation_id: UUID
    lease_token: UUID
    admission_state: str


class DispatchRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def claim_start(self) -> DispatchClaim | None:
        now = datetime.now(UTC)
        run = await self.session.scalar(
            select(Investigation)
            .where(
                Investigation.status == "pending",
                Investigation.admission_state.in_(("not_sent", "unknown")),
                or_(Investigation.next_start_at.is_(None), Investigation.next_start_at <= now),
                or_(Investigation.lease_until.is_(None), Investigation.lease_until < now),
                Investigation.cancel_requested_at.is_(None),
            )
            .order_by(Investigation.created_at)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if run is None:
            return None
        incident = await self.session.get(Incident, run.incident_id)
        assert incident is not None
        service_ids = list(
            await self.session.scalars(
                select(IncidentService.service_id).where(IncidentService.incident_id == incident.id)
            )
        )
        token = uuid7()
        run.lease_token = token
        run.lease_until = now + timedelta(seconds=30)
        request = {
            "schema_version": "1.0",
            "investigation_id": str(run.id),
            "incident_id": str(run.incident_id),
            "project_id": str(run.project_id),
            "environment_id": str(incident.environment_id),
            "service_ids": [str(value) for value in service_ids],
            "detected_at": incident.detected_at.isoformat(),
            "window": {
                "from": (incident.detected_at - timedelta(minutes=30)).isoformat(),
                "to": (incident.detected_at + timedelta(minutes=5)).isoformat(),
            },
            "alert_summary": incident.title,
            "budget": run.budget,
            "attempt": run.attempt,
            "parent_investigation_id": (
                str(run.parent_investigation_id) if run.parent_investigation_id else None
            ),
        }
        return DispatchClaim(run.id, token, run.admission_state, request)

    async def apply_start(
        self,
        claim: DispatchClaim,
        *,
        state: str,
        status: str | None = None,
        retry_after: int = 5,
    ) -> bool:
        run = await self.session.get(Investigation, claim.investigation_id, with_for_update=True)
        if (
            run is None
            or run.lease_token != claim.lease_token
            or run.status not in {"pending", "running"}
        ):
            return False
        now = datetime.now(UTC)
        run.admission_state = state
        run.lease_token = None
        run.lease_until = None
        run.next_start_at = None if state == "admitted" else now + timedelta(seconds=retry_after)
        if status == "running" and run.status == "pending":
            run.status = "running"
            run.started_at = now
        return True

    async def claim_cancel(self) -> CancelClaim | None:
        now = datetime.now(UTC)
        run = await self.session.scalar(
            select(Investigation)
            .where(
                Investigation.status.in_(("pending", "running")),
                Investigation.admission_state.in_(("admitted", "unknown")),
                Investigation.cancel_requested_at.is_not(None),
                Investigation.cancel_delivered_at.is_(None),
                or_(Investigation.next_start_at.is_(None), Investigation.next_start_at <= now),
                or_(Investigation.lease_until.is_(None), Investigation.lease_until < now),
            )
            .order_by(Investigation.cancel_requested_at)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        if run is None:
            return None
        token = uuid7()
        run.lease_token = token
        run.lease_until = now + timedelta(seconds=30)
        return CancelClaim(run.id, token, run.admission_state)

    async def apply_cancel(
        self, claim: CancelClaim, delivered: bool, *, not_found: bool = False
    ) -> tuple[Investigation, Incident] | None:
        current = await self.session.get(Investigation, claim.investigation_id)
        if current is None:
            return None
        incident = await self.session.get(Incident, current.incident_id, with_for_update=True)
        run = await self.session.get(Investigation, claim.investigation_id, with_for_update=True)
        if run is None or run.lease_token != claim.lease_token:
            return None
        run.lease_token = None
        run.lease_until = None
        now = datetime.now(UTC)
        closed_now = False
        if (
            not_found
            and run.deadline is not None
            and now >= run.deadline
            and run.status in {"pending", "running"}
        ):
            run.status = "canceled"
            run.finished_at = now
            closed_now = True
            if incident is not None:
                incident.status = "open"
                incident.version += 1
        if delivered:
            run.cancel_delivered_at = now
        elif run.status in {"pending", "running"}:
            run.next_start_at = now + timedelta(seconds=5)
        if closed_now and incident is not None:
            return run, incident
        return None
