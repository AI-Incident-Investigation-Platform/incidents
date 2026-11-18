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
from src.core.db.models import outbox
from src.core.projects_topology import ProjectsTopology
from src.main import create_app


@pytest.mark.asyncio
@pytest.mark.case(id="INC-MAN-006")
async def test_manual_incident_create_patch_and_resolve() -> None:
    url = os.environ.get("AIIP_TEST_DATABASE_URL")
    if not url:
        pytest.skip("AIIP_TEST_DATABASE_URL is required")
    if not urlparse(url).path.lstrip("/").startswith("incidents_test"):
        raise RuntimeError("Integration test requires an incidents_test database")
    engine = create_async_engine(url)
    project_id, environment_id, user_id, session_id, service_id = (uuid7() for _ in range(5))
    principal = Principal(user_id=user_id, session_id=session_id)
    access = FakeAccessChecker()
    access.allow(project_id, user_id, "project.read", "incident.manage", "report.read")

    async def authenticate(authorization: str | None) -> Principal:
        return principal

    def projects(request: httpx.Request) -> httpx.Response:
        assert request.url.path == f"/api/v1/projects/{project_id}/topology"
        assert request.url.params["environment_id"] == str(environment_id)
        assert request.headers["Authorization"] == "Bearer test-session"
        return httpx.Response(
            200, json={"services": [{"service_id": str(service_id), "status": "healthy"}]}
        )

    try:
        async with engine.connect() as connection:
            transaction = await connection.begin()
            projects_http = httpx.AsyncClient(transport=httpx.MockTransport(projects))
            try:
                sessions = async_sessionmaker(
                    bind=connection, join_transaction_mode="create_savepoint"
                )
                service = IncidentService(
                    sessions, access, topology=ProjectsTopology(projects_http, "http://projects")
                )
                transport = httpx.ASGITransport(app=create_app(service, authenticate))
                async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
                    created = await client.post(
                        "/api/v1/incidents",
                        json={
                            "title": "Ошибка API",
                            "project_id": str(project_id),
                            "environment_id": str(environment_id),
                            "service_ids": [str(service_id)],
                            "severity": "high",
                            "detected_at": datetime.now(UTC).isoformat(),
                        },
                    )
                    assert created.status_code == 201, created.text
                    incident_id = created.json()["id"]
                    changed = await client.patch(
                        f"/api/v1/incidents/{incident_id}",
                        headers={"If-Match": "1"},
                        json={"description": "проверено"},
                    )
                    assert changed.status_code == 200, changed.text
                    assert changed.json()["version"] == 2
                    resolved = await client.patch(
                        f"/api/v1/incidents/{incident_id}/status", json={"status": "resolved"}
                    )
                    assert resolved.status_code == 200, resolved.text
                    assert resolved.json()["status"] == "resolved"
                    timeline = await client.get(f"/api/v1/incidents/{incident_id}/timeline")
                    assert timeline.status_code == 200
                    assert len(timeline.json()["items"]) == 3
                    timeline_page = await client.get(
                        f"/api/v1/incidents/{incident_id}/timeline", params={"limit": 1}
                    )
                    assert timeline_page.status_code == 200
                    assert timeline_page.json()["next_cursor"] is not None
                    timeline_next = await client.get(
                        f"/api/v1/incidents/{incident_id}/timeline",
                        params={"limit": 1, "cursor": timeline_page.json()["next_cursor"]},
                    )
                    assert timeline_next.status_code == 200
                    assert (
                        timeline_next.json()["items"][0]["id"]
                        != timeline_page.json()["items"][0]["id"]
                    )
                    matches = await client.get(
                        "/api/v1/incidents",
                        params={
                            "project_id": str(project_id),
                            "environment_id": str(environment_id),
                            "service_id": str(service_id),
                            "severity": "high",
                            "status": "resolved",
                            "search": "ошибка",
                            "sort": "detected_at_asc",
                        },
                    )
                    assert matches.status_code == 200, matches.text
                    assert [row["id"] for row in matches.json()["items"]] == [incident_id]
                    misses = await client.get(
                        "/api/v1/incidents",
                        params={"project_id": str(project_id), "service_id": str(uuid7())},
                    )
                    assert misses.json()["items"] == []
                    async with sessions() as session:
                        messages = (
                            await session.execute(select(outbox.c.body, outbox.c.destination))
                        ).all()
                        live_resolved = [
                            body
                            for body, destination in messages
                            if destination["exchange"] == "incidents.live"
                            and body["type"] == "incident.updated"
                            and body["data"]["incident"]["id"] == incident_id
                            and body["data"]["incident"]["status"] == "resolved"
                        ]
                        assert len(live_resolved) == 1
                    critical = await client.post(
                        "/api/v1/incidents",
                        json={
                            "title": "Critical incident",
                            "project_id": str(project_id),
                            "environment_id": str(environment_id),
                            "service_ids": [],
                            "severity": "critical",
                            "detected_at": datetime.now(UTC).isoformat(),
                        },
                    )
                    assert critical.status_code == 201, critical.text
                    sorted_page = await client.get(
                        "/api/v1/incidents",
                        params={"project_id": str(project_id), "sort": "severity", "limit": 1},
                    )
                    assert sorted_page.status_code == 200, sorted_page.text
                    assert sorted_page.json()["items"][0]["id"] == critical.json()["id"]
                    assert sorted_page.json()["next_cursor"] is not None
                    sorted_next = await client.get(
                        "/api/v1/incidents",
                        params={
                            "project_id": str(project_id),
                            "sort": "severity",
                            "limit": 1,
                            "cursor": sorted_page.json()["next_cursor"],
                        },
                    )
                    assert sorted_next.status_code == 200, sorted_next.text
                    assert sorted_next.json()["items"][0]["id"] == incident_id
                    overview = await client.get(
                        "/api/v1/overview",
                        params={
                            "project_id": str(project_id),
                            "environment_id": str(environment_id),
                        },
                        headers={"Authorization": "Bearer test-session"},
                    )
                    assert overview.status_code == 200, overview.text
                    counts = overview.json()
                    assert counts["open_incidents"] == counts["critical_incidents"] == 1
                    assert counts["resolved_last_24h"] == 1
                    assert counts["active_investigations"] == 0
                    assert counts["healthy_services"] == counts["total_services"] == 1
            finally:
                await projects_http.aclose()
                await transaction.rollback()
    finally:
        await engine.dispose()
