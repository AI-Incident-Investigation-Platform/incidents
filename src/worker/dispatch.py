import asyncio
import logging
from contextlib import suppress
from uuid import uuid7

from aiip_kit.messaging.message import Actor, MessageV1
from aiip_kit.messaging.topology import Destination, live_project
from aiip_kit.outbox.writer import OutboxWriter
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.api.incidents.service import incident_view
from src.core.ai_client import AiBusy, AiClient, AiUncertain
from src.core.db.dispatch_repository import DispatchClaim, DispatchRepository
from src.core.db.models import outbox
from src.core.db.repository import IncidentRepository

log = logging.getLogger(__name__)


class DispatchWorker:
    def __init__(self, sessions: async_sessionmaker[AsyncSession], ai: AiClient) -> None:
        self._sessions = sessions
        self._ai = ai
        self._outbox = OutboxWriter(outbox)

    async def run_once(self) -> bool:
        async with self._sessions.begin() as session:
            claim = await DispatchRepository(session).claim_start()
        if claim is not None:
            await self._start(claim)
            return True
        async with self._sessions.begin() as session:
            cancel = await DispatchRepository(session).claim_cancel()
        if cancel is None:
            return False
        delivered = False
        not_found = False
        try:
            if cancel.admission_state == "unknown":
                not_found = await self._ai.readback(cancel.investigation_id) is None
            if not not_found:
                await self._ai.cancel(cancel.investigation_id)
                delivered = True
        except AiUncertain:
            log.warning(
                "AI cancel response uncertain",
                extra={"investigation_id": str(cancel.investigation_id)},
            )
        async with self._sessions.begin() as session:
            closed = await DispatchRepository(session).apply_cancel(
                cancel, delivered, not_found=not_found
            )
            if closed is not None:
                run, incident = closed
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
                    lifecycle_version=run.snapshot.get("lifecycle_version"),
                    aggregate_type="investigation",
                    aggregate_id=run.id,
                    actor=Actor(kind="service", id="incidents"),
                )
                await self._outbox.add(
                    session, finished, Destination.event("incidents", "investigation.finished")
                )
                changed = MessageV1.event(
                    "incident.updated",
                    "incidents",
                    {
                        "incident": (
                            await incident_view(IncidentRepository(session), incident)
                        ).model_dump(mode="json"),
                        "changed": ["status"],
                    },
                    project_id=run.project_id,
                    lifecycle_version=run.snapshot.get("lifecycle_version"),
                    aggregate_type="incident",
                    aggregate_id=incident.id,
                    aggregate_version=incident.version,
                    actor=Actor(kind="service", id="incidents"),
                    causation_id=finished.message_id,
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
        return True

    async def _start(self, claim: DispatchClaim) -> None:
        state = "admitted"
        status: str | None = None
        retry_after = 5
        try:
            if claim.admission_state == "unknown":
                status = await self._ai.readback(claim.investigation_id)
            if status is None:
                status = await self._ai.start(claim.request)
        except AiBusy as exc:
            state = "not_sent"
            retry_after = exc.retry_after
        except AiUncertain:
            state = "unknown"
        async with self._sessions.begin() as session:
            await DispatchRepository(session).apply_start(
                claim, state=state, status=status, retry_after=retry_after
            )

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                worked = await self.run_once()
            except Exception:
                log.exception("Dispatch worker failed")
                worked = False
            if not worked:
                with suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=1.0)
