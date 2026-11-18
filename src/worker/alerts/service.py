from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid7

from aiip_kit.messaging.message import Actor, MessageV1
from aiip_kit.messaging.topology import Destination, live_project
from aiip_kit.outbox.inbox import InboxGuard
from aiip_kit.outbox.writer import OutboxWriter
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.api.incidents.service import incident_view
from src.api.investigations.schemas import ExecutionBudget
from src.api.investigations.service import InvestigationService
from src.core.db.alert_repository import AlertRepository
from src.core.db.investigation_repository import InvestigationRepository
from src.core.db.models import (
    Alert,
    AutostartGate,
    CorrelationSlot,
    Incident,
    IngestReceipt,
    Investigation,
    inbox,
    outbox,
)
from src.core.db.repository import IncidentRepository
from src.worker.alerts.rules import (
    correlation_key,
    parse_alert,
    payload_hash,
    same_episode,
    select_rule,
    should_autostart,
)


class AlertWorker:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions
        self._inbox = InboxGuard(inbox)
        self._outbox = OutboxWriter(outbox)

    async def handle(self, message: MessageV1, attempt: int) -> None:
        if message.type != "alert.received" or message.project_id is None:
            raise ValueError("Неизвестная команда")
        alert = parse_alert(message.project_id, message.data)
        receipt_id = UUID(message.data["receipt_id"])
        token_id = UUID(message.data["token_id"])
        received_at = datetime.fromisoformat(message.data["received_at"])
        now = datetime.now(UTC)
        async with self._sessions.begin() as session:
            if not await self._inbox.accept(session, "alert.received", message):
                return
            repo = AlertRepository(session)
            await repo.lock_project(message.project_id)
            if await repo.tombstone(message.project_id) is not None:
                return
            if await repo.receipt(receipt_id) is not None:
                return
            receipt = IngestReceipt(
                id=receipt_id,
                project_id=message.project_id,
                token_id=token_id,
                kind="alert",
                status="queued",
                received_at=received_at,
            )
            session.add(receipt)
            if message.expires_at and now > message.expires_at:
                self._reject(receipt, "expired", now)
                return
            if now - received_at > timedelta(minutes=15):
                self._reject(receipt, "expired", now)
                return
            digest = payload_hash(alert)
            existing = await repo.alert(message.project_id, alert.source, alert.source_event_id)
            if existing is not None:
                receipt.status = "duplicate" if existing.payload_hash == digest else "rejected"
                receipt.reason = (
                    None if existing.payload_hash == digest else "incompatible_duplicate"
                )
                receipt.resource_id = existing.id
                receipt.incident_id = existing.incident_id
                receipt.processed_at = now
                return
            config = await repo.latest_config(message.project_id)
            snapshot = config.snapshot if config else {}
            rule = select_rule(alert, snapshot.get("rules", []))
            daily_limit = int(snapshot.get("alert_daily_limit", 10_000))
            if not await repo.consume_quota(message.project_id, now.date(), daily_limit):
                self._reject(receipt, "daily_quota_exceeded", now)
                return
            correlation = rule.get("correlation", {}) if rule else {}
            key = correlation_key(alert, correlation.get("keys") if rule else None)
            slot = await repo.slot(message.project_id, key)
            if slot is not None and same_episode(slot.episode_until, now):
                incident_id = slot.incident_id
                incident = await IncidentRepository(session).get(incident_id, lock=True)
                assert incident is not None
            else:
                hourly_limit = snapshot.get("budgets", {}).get("max_incidents_per_hour")
                if hourly_limit is not None and not await repo.consume_quota(
                    message.project_id, now.date(), int(hourly_limit), f"incidents:{now.hour:02d}"
                ):
                    self._reject(receipt, "incident_quota_exceeded", now)
                    return
                incident_repo = IncidentRepository(session)
                incident = Incident(
                    project_id=message.project_id,
                    environment_id=alert.environment_id,
                    display_number=await incident_repo.next_display_number(message.project_id),
                    title=alert.title,
                    description=alert.description,
                    severity=alert.severity,
                    status="open",
                    source=alert.source,
                    detected_at=alert.started_at,
                    version=1,
                )
                await incident_repo.add(incident)
                await incident_repo.replace_services(incident.id, alert.service_ids)
                await incident_repo.add_timeline(
                    incident.id, "incident.created", "Инцидент создан", "Алерт получен"
                )
                incident_id = incident.id
                episode_until = now + timedelta(
                    seconds=int(
                        correlation.get(
                            "window_seconds", snapshot.get("correlation_window_seconds", 900)
                        )
                    )
                )
                if slot is None:
                    session.add(
                        CorrelationSlot(
                            project_id=message.project_id,
                            correlation_key=key,
                            incident_id=incident_id,
                            episode_until=episode_until,
                        )
                    )
                else:
                    slot.incident_id = incident_id
                    slot.episode_until = episode_until
                event = message.caused(
                    "incident.created",
                    "incidents",
                    {
                        "incident": {
                            "id": str(incident.id),
                            "title": incident.title,
                            "description": incident.description,
                            "project_id": str(incident.project_id),
                            "environment_id": str(incident.environment_id),
                            "service_ids": [str(value) for value in alert.service_ids],
                            "severity": incident.severity,
                            "status": incident.status,
                            "source": incident.source,
                            "detected_at": incident.detected_at.isoformat(),
                            "created_at": incident.created_at.isoformat(),
                            "updated_at": incident.updated_at.isoformat(),
                            "resolved_at": None,
                            "latest_investigation_id": None,
                            "alert_fingerprint": key,
                        }
                    },
                    kind="event",
                    aggregate_type="incident",
                    aggregate_id=incident_id,
                    aggregate_version=1,
                )
                await self._outbox.add(
                    session, event, Destination.event("incidents", "incident.created")
                )
                await self._outbox.add(
                    session,
                    event.model_copy(
                        update={"message_id": uuid7(), "causation_id": event.message_id}
                    ),
                    Destination.live("incidents", live_project(incident.project_id)),
                )
            item = Alert(
                project_id=message.project_id,
                environment_id=alert.environment_id,
                token_id=token_id,
                source=alert.source,
                source_event_id=alert.source_event_id,
                payload_hash=digest,
                payload=alert.model_dump(mode="json"),
                incident_id=incident_id,
                started_at=alert.started_at,
                received_at=received_at,
            )
            session.add(item)
            await session.flush()
            receipt.status = "accepted"
            receipt.resource_id = item.id
            receipt.incident_id = incident_id
            receipt.processed_at = now
            if rule is not None and should_autostart(alert, rule.get("autostart", {})):
                await self._autostart(session, repo, message, incident, snapshot, rule, now)

    async def _autostart(
        self,
        session: AsyncSession,
        repo: AlertRepository,
        message: MessageV1,
        incident: Incident,
        snapshot: dict,
        rule: dict,
        now: datetime,
    ) -> None:
        if incident.status == "resolved":
            return
        incident_repo = IncidentRepository(session)
        if await incident_repo.active_investigation(incident.id):
            return
        gate = await session.get(AutostartGate, (incident.project_id, incident.id))
        if gate is not None and gate.next_allowed_at > now:
            return
        budgets = snapshot.get("budgets", {})
        daily_limit = int(budgets.get("max_investigations_per_day", 100))
        if not await repo.consume_quota(
            incident.project_id, now.date(), daily_limit, "investigations"
        ):
            return
        budget = ExecutionBudget.model_validate(budgets.get("agent_budget", {}))
        history = await InvestigationRepository(session).history(incident.id)
        run = Investigation(
            incident_id=incident.id,
            project_id=incident.project_id,
            attempt=max((item.attempt for item in history), default=0) + 1,
            status="pending",
            snapshot={
                "incident_id": str(incident.id),
                "version": incident.version,
                "title": incident.title,
                "severity": incident.severity,
                "lifecycle_version": message.lifecycle_version,
                "rule_id": rule.get("id"),
            },
            budget=budget.model_dump(),
            context_note=None,
            admission_state="not_sent",
            created_at=now,
            deadline=now + timedelta(seconds=budget.timeout_seconds),
            current_step=0,
            tool_calls_count=0,
        )
        session.add(run)
        cooldown = max(0, int(rule.get("autostart", {}).get("cooldown_seconds", 0)))
        if gate is None:
            session.add(
                AutostartGate(
                    project_id=incident.project_id,
                    incident_id=incident.id,
                    next_allowed_at=now + timedelta(seconds=cooldown),
                )
            )
        else:
            gate.next_allowed_at = now + timedelta(seconds=cooldown)
        incident.status = "investigating"
        incident.updated_at = now
        incident.version += 1
        await session.flush()
        created = message.caused(
            "investigation.created",
            "incidents",
            {"investigation": InvestigationService._view(run, incident).model_dump(mode="json")},
            kind="event",
            aggregate_type="investigation",
            aggregate_id=run.id,
            aggregate_version=1,
            actor=Actor(kind="service", id="incidents"),
        )
        await self._outbox.add(
            session, created, Destination.event("incidents", "investigation.created")
        )
        changed = message.caused(
            "incident.updated",
            "incidents",
            {
                "incident": (await incident_view(incident_repo, incident)).model_dump(mode="json"),
                "changed": ["status"],
            },
            kind="event",
            aggregate_type="incident",
            aggregate_id=incident.id,
            aggregate_version=incident.version,
            actor=Actor(kind="service", id="incidents"),
        )
        await self._outbox.add(session, changed, Destination.event("incidents", "incident.updated"))
        await self._outbox.add(
            session,
            changed.model_copy(update={"message_id": uuid7(), "causation_id": changed.message_id}),
            Destination.live("incidents", live_project(incident.project_id)),
        )

    @staticmethod
    def _reject(receipt: IngestReceipt, reason: str, now: datetime) -> None:
        receipt.status = "rejected"
        receipt.reason = reason
        receipt.processed_at = now
