import os
from datetime import UTC, datetime
from urllib.parse import urlparse
from uuid import uuid7

import httpx
import pytest
from aiip_kit.access.checker import FakeAccessChecker
from aiip_kit.access.model import Principal
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.api.incidents.service import IncidentService
from src.api.investigations.service import InvestigationService
from src.core.db.models import outbox
from src.main import create_app


@pytest.mark.asyncio
@pytest.mark.case(id="INC-INV-003")
async def test_start_is_idempotent_and_retry_creates_next_attempt() -> None:
    url = os.environ.get("AIIP_TEST_DATABASE_URL")
    if not url:
        pytest.skip("AIIP_TEST_DATABASE_URL is required")
    if not urlparse(url).path.lstrip("/").startswith("incidents_test"):
        raise RuntimeError("Integration test requires an incidents_test database")
    engine = create_async_engine(url)
    project_id, environment_id, user_id, session_id = (uuid7() for _ in range(4))
    principal = Principal(user_id=user_id, session_id=session_id)
    access = FakeAccessChecker()
    access.allow(project_id, user_id, "project.read", "incident.manage", "investigation.run")

    async def authenticate(authorization: str | None) -> Principal:
        return principal

    try:
        async with engine.connect() as connection:
            transaction = await connection.begin()
            try:
                sessions = async_sessionmaker(
                    bind=connection, join_transaction_mode="create_savepoint"
                )
                app = create_app(
                    IncidentService(sessions, access),
                    authenticate,
                    InvestigationService(sessions, access),
                )
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://test"
                ) as client:
                    created = await client.post(
                        "/api/v1/incidents",
                        json={
                            "title": "Latency",
                            "project_id": str(project_id),
                            "environment_id": str(environment_id),
                            "service_ids": [],
                            "severity": "high",
                            "detected_at": datetime.now(UTC).isoformat(),
                        },
                    )
                    assert created.status_code == 201, created.text
                    incident_id = created.json()["id"]
                    first = await client.post(
                        f"/api/v1/incidents/{incident_id}/investigations",
                        headers={"Idempotency-Key": "start-1"},
                        json={},
                    )
                    assert first.status_code == 202, first.text
                    first_id = first.json()["id"]
                    repeated = await client.post(
                        f"/api/v1/incidents/{incident_id}/investigations",
                        headers={"Idempotency-Key": "start-1"},
                        json={},
                    )
                    assert repeated.status_code == 202, repeated.text
                    assert repeated.json()["id"] == first_id
                    conflict = await client.post(
                        f"/api/v1/incidents/{incident_id}/investigations",
                        headers={"Idempotency-Key": "start-2"},
                        json={},
                    )
                    assert conflict.status_code == 409, conflict.text
                    canceled = await client.post(f"/api/v1/investigations/{first_id}/cancel")
                    assert canceled.status_code == 202, canceled.text
                    assert canceled.json()["status"] == "canceled"
                    async with sessions() as session:
                        messages = (await session.execute(select(outbox.c.body))).scalars().all()
                        assert any(
                            body["type"] == "investigation.finished"
                            and body["data"]["investigation_id"] == first_id
                            and body["data"]["status"] == "canceled"
                            for body in messages
                        )
                    retry = await client.post(
                        f"/api/v1/investigations/{first_id}/retry",
                        headers={"Idempotency-Key": "retry-1"},
                    )
                    assert retry.status_code == 202, retry.text
                    assert retry.json()["attempt"] == 2
                    assert retry.json()["id"] != first_id
                    page = await client.get(
                        "/api/v1/investigations",
                        params={"project_id": str(project_id), "limit": 1},
                    )
                    assert page.status_code == 200, page.text
                    assert page.json()["items"][0]["id"] == retry.json()["id"]
                    assert page.json()["next_cursor"] is not None
                    next_page = await client.get(
                        "/api/v1/investigations",
                        params={
                            "project_id": str(project_id),
                            "limit": 1,
                            "cursor": page.json()["next_cursor"],
                        },
                    )
                    assert next_page.status_code == 200, next_page.text
                    assert next_page.json()["items"][0]["id"] == first_id
                    filtered = await client.get(
                        "/api/v1/investigations",
                        params={"project_id": str(project_id), "status": "canceled"},
                    )
                    assert [item["id"] for item in filtered.json()["items"]] == [first_id]
                    resolving = await client.patch(
                        f"/api/v1/incidents/{incident_id}/status", json={"status": "resolved"}
                    )
                    assert resolving.status_code == 409, resolving.text
            finally:
                await transaction.rollback()
    finally:
        await engine.dispose()
