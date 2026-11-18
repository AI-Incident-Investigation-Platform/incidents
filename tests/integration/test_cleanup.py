import os
from datetime import UTC, datetime
from urllib.parse import urlparse
from uuid import uuid7

import pytest
from aiip_kit.access.checker import FakeAccessChecker
from aiip_kit.access.model import Principal
from aiip_kit.errors import InboxConflictError, ProjectNotFoundError
from aiip_kit.messaging.message import MessageV1
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.api.incidents.schemas import IncidentCreate
from src.api.incidents.service import IncidentService
from src.api.investigations.schemas import InvestigationCreate
from src.api.investigations.service import InvestigationService
from src.core.db.models import Incident, ProjectTombstone, outbox
from src.worker.cleanup import CleanupWorker


@pytest.mark.asyncio
@pytest.mark.case(id="INC-DEL-001")
async def test_project_cleanup_removes_rows_and_writes_ack() -> None:
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
                        title="Cleanup case",
                        project_id=project_id,
                        environment_id=environment_id,
                        service_ids=[],
                        severity="low",
                        detected_at=datetime.now(UTC),
                    ),
                )
                await InvestigationService(sessions, access).start(
                    principal, incident.id, InvestigationCreate(), "cleanup-start"
                )
                command = MessageV1.event(
                    "project.deleting",
                    "projects",
                    {"project_id": str(project_id)},
                    project_id=project_id,
                    lifecycle_version=2,
                )
                worker = CleanupWorker(sessions)
                await worker.handle(command, 1)
                await worker.handle(command, 2)
                another_project = uuid7()
                corrupted_replay = command.model_copy(
                    update={
                        "project_id": another_project,
                        "data": {"project_id": str(another_project)},
                    }
                )
                with pytest.raises(InboxConflictError):
                    await worker.handle(corrupted_replay, 3)
                with pytest.raises(ProjectNotFoundError):
                    await IncidentService(sessions, access).create(
                        principal,
                        IncidentCreate(
                            title="Late incident",
                            project_id=project_id,
                            environment_id=environment_id,
                            service_ids=[],
                            severity="low",
                            detected_at=datetime.now(UTC),
                        ),
                    )
                async with sessions() as session:
                    assert await session.get(Incident, incident.id) is None
                    marker = await session.get(ProjectTombstone, project_id)
                    assert marker is not None and marker.lifecycle_version == 2
                    assert await session.get(ProjectTombstone, another_project) is None
                    rows = (await session.execute(select(outbox.c.body))).all()
                    matches = [
                        row.body
                        for row in rows
                        if row.body.get("type") == "cleanup.completed"
                        and row.body.get("project_id") == str(project_id)
                    ]
                    assert len(matches) == 1
            finally:
                await transaction.rollback()
    finally:
        await engine.dispose()
