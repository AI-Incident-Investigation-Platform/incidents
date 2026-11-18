import os
from datetime import UTC, datetime, timedelta
from urllib.parse import urlparse
from uuid import uuid7

import pytest
from aiip_kit.access.checker import FakeAccessChecker
from aiip_kit.access.model import Principal
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.api.incidents.schemas import IncidentCreate
from src.api.incidents.service import IncidentService
from src.api.investigations.schemas import InvestigationCreate
from src.api.investigations.service import InvestigationService
from src.core.ai_client import FakeAiClient
from src.core.db.investigation_repository import InvestigationRepository
from src.core.db.models import outbox
from src.worker.dispatch import DispatchWorker


@pytest.mark.asyncio
@pytest.mark.case(id="INC-AI-002")
async def test_lost_start_response_uses_readback_before_retry() -> None:
    url = os.environ.get("AIIP_TEST_DATABASE_URL")
    if not url:
        pytest.skip("AIIP_TEST_DATABASE_URL is required")
    if not urlparse(url).path.lstrip("/").startswith("incidents_test"):
        raise RuntimeError("Integration test requires an incidents_test database")
    engine = create_async_engine(url)
    project_id, environment_id, user_id, session_id = (uuid7() for _ in range(4))
    principal = Principal(user_id=user_id, session_id=session_id)
    access = FakeAccessChecker()
    access.allow(project_id, user_id, "incident.manage", "investigation.run")
    try:
        async with engine.connect() as connection:
            transaction = await connection.begin()
            try:
                sessions = async_sessionmaker(
                    bind=connection, join_transaction_mode="create_savepoint"
                )
                incident = await IncidentService(sessions, access).create(
                    principal,
                    IncidentCreate(
                        title="CPU high",
                        project_id=project_id,
                        environment_id=environment_id,
                        service_ids=[],
                        severity="high",
                        detected_at=datetime.now(UTC),
                    ),
                )
                run = await InvestigationService(sessions, access).start(
                    principal, incident.id, InvestigationCreate(), "lost-response"
                )
                ai = FakeAiClient()
                ai.lose_start_response = True
                worker = DispatchWorker(sessions, ai)
                assert await worker.run_once()
                async with sessions.begin() as session:
                    row = await InvestigationRepository(session).get(run.id)
                    assert row is not None and row.admission_state == "unknown"
                    row.next_start_at = datetime.now(UTC) - timedelta(seconds=1)
                assert await worker.run_once()
                async with sessions() as session:
                    row = await InvestigationRepository(session).get(run.id)
                    assert row is not None and row.admission_state == "admitted"
                assert len(ai.starts) == 1
            finally:
                await transaction.rollback()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.case(id="INC-AI-003")
async def test_cancel_after_lost_start_response_reaches_ai() -> None:
    url = os.environ.get("AIIP_TEST_DATABASE_URL")
    if not url:
        pytest.skip("AIIP_TEST_DATABASE_URL is required")
    if not urlparse(url).path.lstrip("/").startswith("incidents_test"):
        raise RuntimeError("Integration test requires an incidents_test database")
    engine = create_async_engine(url)
    project_id, environment_id, user_id, session_id = (uuid7() for _ in range(4))
    principal = Principal(user_id=user_id, session_id=session_id)
    access = FakeAccessChecker()
    access.allow(project_id, user_id, "incident.manage", "investigation.run")
    try:
        async with engine.connect() as connection:
            transaction = await connection.begin()
            try:
                sessions = async_sessionmaker(
                    bind=connection, join_transaction_mode="create_savepoint"
                )
                incident = await IncidentService(sessions, access).create(
                    principal,
                    IncidentCreate(
                        title="Cancel uncertain start",
                        project_id=project_id,
                        environment_id=environment_id,
                        service_ids=[],
                        severity="high",
                        detected_at=datetime.now(UTC),
                    ),
                )
                service = InvestigationService(sessions, access)
                run = await service.start(
                    principal, incident.id, InvestigationCreate(), "cancel-unknown"
                )
                ai = FakeAiClient()
                ai.lose_start_response = True
                worker = DispatchWorker(sessions, ai)
                assert await worker.run_once()
                await service.cancel(principal, run.id)
                assert await worker.run_once()
                assert ai.cancels == [run.id]
                async with sessions() as session:
                    row = await InvestigationRepository(session).get(run.id)
                    assert row is not None and row.cancel_delivered_at is not None
                second_incident = await IncidentService(sessions, access).create(
                    principal,
                    IncidentCreate(
                        title="Start never admitted",
                        project_id=project_id,
                        environment_id=environment_id,
                        service_ids=[],
                        severity="high",
                        detected_at=datetime.now(UTC),
                    ),
                )
                second = await service.start(
                    principal, second_incident.id, InvestigationCreate(), "cancel-not-found"
                )
                async with sessions.begin() as session:
                    row = await InvestigationRepository(session).get(second.id)
                    assert row is not None
                    row.admission_state = "unknown"
                    row.deadline = datetime.now(UTC) - timedelta(seconds=1)
                await service.cancel(principal, second.id)
                assert await worker.run_once()
                async with sessions() as session:
                    row = await InvestigationRepository(session).get(second.id)
                    assert row is not None and row.status == "canceled"
                    messages = (await session.execute(select(outbox.c.body))).scalars().all()
                    assert any(
                        body["type"] == "investigation.finished"
                        and body["data"]["investigation_id"] == str(second.id)
                        for body in messages
                    )
            finally:
                await transaction.rollback()
    finally:
        await engine.dispose()
