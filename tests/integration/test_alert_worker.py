import os
from datetime import UTC, datetime, timedelta
from urllib.parse import urlparse
from uuid import uuid7

import pytest
from aiip_kit.access.checker import FakeAccessChecker
from aiip_kit.access.model import Principal
from aiip_kit.messaging.message import MessageV1
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.api.reports.service import ReportService
from src.core.db.models import (
    CorrelationSlot,
    Incident,
    IngestReceipt,
    Investigation,
    ProjectConfig,
)
from src.worker.alerts.service import AlertWorker


@pytest.mark.asyncio
@pytest.mark.case(id="INC-ALT-003")
async def test_alert_deduplication_and_new_episode() -> None:
    url = os.environ.get("AIIP_TEST_DATABASE_URL")
    if not url:
        pytest.skip("AIIP_TEST_DATABASE_URL is required")
    if not urlparse(url).path.lstrip("/").startswith("incidents_test"):
        raise RuntimeError("Integration test requires an incidents_test database")
    engine = create_async_engine(url)
    project_id, environment_id, token_id = (uuid7() for _ in range(3))
    now = datetime.now(UTC)

    def command(receipt_id: str, source_event_id: str) -> MessageV1:
        return MessageV1.command(
            "alert.received",
            "ingest",
            {
                "receipt_id": receipt_id,
                "token_id": str(token_id),
                "environment_id": str(environment_id),
                "format": "aiip",
                "payload": {
                    "source": "prometheus",
                    "source_event_id": source_event_id,
                    "project_id": str(project_id),
                    "environment_id": str(environment_id),
                    "service_ids": [],
                    "severity": "high",
                    "title": "CPU high",
                    "started_at": now.isoformat(),
                    "labels": {"alertname": "CPUHigh"},
                },
                "received_at": now.isoformat(),
            },
            project_id=project_id,
        )

    try:
        async with engine.connect() as connection:
            transaction = await connection.begin()
            try:
                sessions = async_sessionmaker(
                    bind=connection, join_transaction_mode="create_savepoint"
                )
                worker = AlertWorker(sessions)
                receipt1, receipt2, receipt3 = (str(uuid7()) for _ in range(3))
                first = command(receipt1, "event-1")
                await worker.handle(first, 1)
                await worker.handle(first, 2)
                await worker.handle(command(receipt2, "event-1"), 1)
                async with sessions() as session:
                    accepted = await session.get(IngestReceipt, receipt1)
                    duplicate = await session.get(IngestReceipt, receipt2)
                    assert accepted is not None and accepted.status == "accepted"
                    assert duplicate is not None and duplicate.status == "duplicate"
                    assert accepted.incident_id == duplicate.incident_id
                async with sessions.begin() as session:
                    slot = await session.scalar(
                        select(CorrelationSlot).where(CorrelationSlot.project_id == project_id)
                    )
                    assert slot is not None
                    slot.episode_until = now - timedelta(seconds=1)
                await worker.handle(command(receipt3, "event-2"), 1)
                async with sessions() as session:
                    later = await session.get(IngestReceipt, receipt3)
                    assert later is not None and later.status == "accepted"
                    assert later.incident_id != accepted.incident_id
            finally:
                await transaction.rollback()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.case(id="INC-ALT-007")
async def test_matching_rule_autostarts_only_one_active_investigation() -> None:
    url = os.environ.get("AIIP_TEST_DATABASE_URL")
    if not url:
        pytest.skip("AIIP_TEST_DATABASE_URL is required")
    if not urlparse(url).path.lstrip("/").startswith("incidents_test"):
        raise RuntimeError("Integration test requires an incidents_test database")
    engine = create_async_engine(url)
    project_id, environment_id, token_id = (uuid7() for _ in range(3))
    now = datetime.now(UTC)
    try:
        async with engine.connect() as connection:
            transaction = await connection.begin()
            try:
                sessions = async_sessionmaker(
                    bind=connection, join_transaction_mode="create_savepoint"
                )
                async with sessions.begin() as session:
                    session.add(
                        ProjectConfig(
                            project_id=project_id,
                            version=1,
                            snapshot={
                                "rules": [
                                    {
                                        "id": str(uuid7()),
                                        "priority": 10,
                                        "match": {"labels": {"alertname": "CPUHigh"}},
                                        "correlation": {
                                            "keys": ["alertname"],
                                            "window_seconds": 900,
                                        },
                                        "autostart": {
                                            "enabled": True,
                                            "cooldown_seconds": 60,
                                            "min_severity": "high",
                                        },
                                    }
                                ],
                                "budgets": {
                                    "max_investigations_per_day": 5,
                                    "max_incidents_per_hour": 5,
                                    "agent_budget": {
                                        "max_steps": 20,
                                        "max_tool_calls": 40,
                                        "timeout_seconds": 1200,
                                    },
                                },
                            },
                        )
                    )
                worker = AlertWorker(sessions)
                for number in (1, 2):
                    command = MessageV1.command(
                        "alert.received",
                        "ingest",
                        {
                            "receipt_id": str(uuid7()),
                            "token_id": str(token_id),
                            "environment_id": str(environment_id),
                            "format": "aiip",
                            "payload": {
                                "source": "prometheus",
                                "source_event_id": f"event-{number}",
                                "project_id": str(project_id),
                                "environment_id": str(environment_id),
                                "service_ids": [],
                                "severity": "high",
                                "title": "CPU high",
                                "started_at": now.isoformat(),
                                "labels": {"alertname": "CPUHigh", "pod": str(number)},
                            },
                            "received_at": now.isoformat(),
                        },
                        project_id=project_id,
                        lifecycle_version=1,
                    )
                    await worker.handle(command, 1)
                async with sessions() as session:
                    incidents = (
                        await session.scalars(
                            select(Incident).where(Incident.project_id == project_id)
                        )
                    ).all()
                    runs = (
                        await session.scalars(
                            select(Investigation).where(Investigation.project_id == project_id)
                        )
                    ).all()
                    assert len(incidents) == 1
                    assert incidents[0].status == "investigating"
                    assert len(runs) == 1 and runs[0].status == "pending"
                user_id, session_id = uuid7(), uuid7()
                access = FakeAccessChecker()
                access.allow(project_id, user_id, "project.read")
                reports = ReportService(sessions, access)
                principal = Principal(user_id=user_id, session_id=session_id)
                first_page = await reports.alerts(principal, incidents[0].id, 1, None)
                assert len(first_page["items"]) == 1
                assert first_page["next_cursor"] is not None
                second_page = await reports.alerts(
                    principal, incidents[0].id, 1, first_page["next_cursor"]
                )
                assert len(second_page["items"]) == 1
                assert second_page["next_cursor"] is None
                assert first_page["items"][0]["id"] != second_page["items"][0]["id"]
            finally:
                await transaction.rollback()
    finally:
        await engine.dispose()
