import base64
import binascii
import json
from datetime import datetime
from uuid import UUID

from aiip_kit.access.checker import AccessChecker
from aiip_kit.access.model import Principal
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.core.db.alert_repository import AlertRepository
from src.core.db.callback_repository import CallbackRepository
from src.core.db.investigation_repository import InvestigationRepository
from src.core.db.repository import IncidentRepository


class ReportNotFound(Exception):
    pass


class ReportService:
    def __init__(self, sessions: async_sessionmaker[AsyncSession], access: AccessChecker) -> None:
        self._sessions = sessions
        self._access = access

    async def _run_project(self, investigation_id: UUID) -> UUID:
        async with self._sessions() as session:
            run = await InvestigationRepository(session).get(investigation_id)
            if run is None:
                raise ReportNotFound
            return run.project_id

    async def _incident_project(self, incident_id: UUID) -> UUID:
        async with self._sessions() as session:
            incident = await IncidentRepository(session).get(incident_id)
            if incident is None:
                raise ReportNotFound
            return incident.project_id

    async def report(self, principal: Principal, investigation_id: UUID) -> dict:
        await self._access.require(
            principal, await self._run_project(investigation_id), "report.read"
        )
        return (await self.internal_report(investigation_id))["report"]

    async def internal_report(self, investigation_id: UUID) -> dict:
        async with self._sessions() as session:
            repo = CallbackRepository(session)
            result = await repo.result(investigation_id)
            if result is None or not isinstance(result.payload.get("report"), dict):
                raise ReportNotFound
            run = await InvestigationRepository(session).get(investigation_id)
            assert run is not None
            incident = await IncidentRepository(session).get(run.incident_id)
            assert incident is not None
            return {
                "report": result.payload["report"],
                "incident": {
                    "id": str(incident.id),
                    "project_id": str(incident.project_id),
                    "title": incident.title,
                    "severity": incident.severity,
                    "status": incident.status,
                },
            }

    async def events(
        self, principal: Principal, investigation_id: UUID, *, after_sequence: int, limit: int
    ) -> dict:
        await self._access.require(
            principal, await self._run_project(investigation_id), "project.read"
        )
        async with self._sessions() as session:
            rows = await CallbackRepository(session).events(investigation_id, after_sequence, limit)
            return {"items": [row.payload for row in rows], "next_cursor": None}

    async def hypotheses(self, principal: Principal, investigation_id: UUID) -> dict:
        await self._access.require(
            principal, await self._run_project(investigation_id), "report.read"
        )
        async with self._sessions() as session:
            result = await CallbackRepository(session).result(investigation_id)
            return {
                "items": result.payload.get("hypotheses", []) if result else [],
                "next_cursor": None,
            }

    async def evidence(
        self, principal: Principal, investigation_id: UUID, limit: int, cursor: str | None
    ) -> dict:
        await self._access.require(
            principal, await self._run_project(investigation_id), "report.read"
        )
        after = None
        if cursor is not None:
            try:
                after = UUID(base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)).decode())
            except (ValueError, UnicodeDecodeError, binascii.Error) as exc:
                raise ValueError("Неверный cursor") from exc
        async with self._sessions() as session:
            rows = await CallbackRepository(session).evidence(
                investigation_id, after=after, limit=limit + 1
            )
            has_more = len(rows) > limit
            rows = rows[:limit]
            next_cursor = (
                base64.urlsafe_b64encode(str(rows[-1].id).encode()).decode().rstrip("=")
                if has_more
                else None
            )
            return {"items": [row.data for row in rows], "next_cursor": next_cursor}

    async def evidence_item(self, principal: Principal, evidence_id: UUID) -> dict:
        async with self._sessions() as session:
            item = await CallbackRepository(session).evidence_by_id(evidence_id)
            if item is None:
                raise ReportNotFound
            investigation_id = item.investigation_id
            data = item.data
        await self._access.require(
            principal, await self._run_project(investigation_id), "report.read"
        )
        return data

    async def alerts(
        self, principal: Principal, incident_id: UUID, limit: int, cursor: str | None
    ) -> dict:
        await self._access.require(
            principal, await self._incident_project(incident_id), "project.read"
        )
        before = None
        if cursor is not None:
            try:
                raw = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4))
                timestamp, alert_id = json.loads(raw)
                before = datetime.fromisoformat(timestamp), UUID(alert_id)
            except (ValueError, TypeError, UnicodeDecodeError, binascii.Error) as exc:
                raise ValueError("Неверный cursor") from exc
        async with self._sessions() as session:
            rows = await AlertRepository(session).incident_alerts(
                incident_id, limit + 1, before=before
            )
            has_more = len(rows) > limit
            rows = rows[:limit]
            next_cursor = None
            if has_more:
                data = [rows[-1].received_at.isoformat(), str(rows[-1].id)]
                next_cursor = (
                    base64.urlsafe_b64encode(json.dumps(data).encode()).decode().rstrip("=")
                )
            return {
                "items": [
                    {
                        "id": str(row.id),
                        "incident_id": str(row.incident_id),
                        "source": row.source,
                        "source_event_id": row.source_event_id,
                        "severity": row.payload["severity"],
                        "title": row.payload["title"],
                        "started_at": row.started_at.isoformat(),
                        "received_at": row.received_at.isoformat(),
                        "labels": row.payload.get("labels", {}),
                    }
                    for row in rows
                ],
                "next_cursor": next_cursor,
            }
