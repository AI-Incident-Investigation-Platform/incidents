import os
from datetime import UTC, datetime
from urllib.parse import urlparse
from uuid import UUID, uuid7

import httpx
import pytest
from aiip_kit.access.checker import FakeAccessChecker
from aiip_kit.access.model import Principal
from aiip_kit.service_auth.fake import FakeServiceTokenVerifier
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.api.ai_callbacks.schemas import AIResultIn, ContextDeployment, ContextService
from src.api.ai_callbacks.service import CallbackService
from src.api.incidents.service import IncidentService
from src.api.investigations.service import InvestigationService
from src.api.reports.service import ReportService
from src.core.db.callback_repository import CallbackRepository
from src.core.db.models import outbox
from src.main import create_app


@pytest.mark.asyncio
@pytest.mark.case(id="INC-AI-001")
async def test_first_terminal_result_wins_and_review_does_not_change_report() -> None:
    url = os.environ.get("AIIP_TEST_DATABASE_URL")
    if not url:
        pytest.skip("AIIP_TEST_DATABASE_URL is required")
    if not urlparse(url).path.lstrip("/").startswith("incidents_test"):
        raise RuntimeError("Integration test requires an incidents_test database")
    engine = create_async_engine(url)
    project_id, environment_id, user_id, session_id = (uuid7() for _ in range(4))
    service_id = uuid7()
    principal = Principal(user_id=user_id, session_id=session_id)
    access = FakeAccessChecker()
    access.allow(
        project_id,
        user_id,
        "project.read",
        "incident.manage",
        "investigation.run",
        "review.write",
        "report.read",
    )

    async def authenticate(authorization: str | None) -> Principal:
        return principal

    class ContextSource:
        async def fetch(
            self,
            project: UUID,
            environment: UUID,
            services: list[UUID],
            start: datetime,
            end: datetime,
        ) -> tuple[list[ContextService], list[ContextDeployment]]:
            assert (project, environment, services) == (project_id, environment_id, [service_id])
            service = ContextService(
                id=service_id,
                project_id=project,
                environment_id=environment,
                name="database",
                external_names=[],
                created_at=start,
            )
            deployment = ContextDeployment(
                id=uuid7(),
                project_id=project,
                environment_id=environment,
                service_id=service_id,
                version="1.0",
                deployed_at=end,
                status="succeeded",
                source="test",
            )
            return [service, service.model_copy(update={"project_id": uuid7()})], [
                deployment,
                deployment.model_copy(update={"environment_id": uuid7()}),
            ]

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
                    callbacks=CallbackService(sessions, ContextSource()),
                    service_verifier=FakeServiceTokenVerifier("main-ai-sink"),
                    reports=ReportService(sessions, access),
                )
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://test"
                ) as client:
                    created = await client.post(
                        "/api/v1/incidents",
                        json={
                            "title": "Database outage",
                            "project_id": str(project_id),
                            "environment_id": str(environment_id),
                            "service_ids": [str(service_id)],
                            "severity": "critical",
                            "detected_at": datetime.now(UTC).isoformat(),
                        },
                    )
                    assert created.status_code == 201, created.text
                    incident_id = UUID(created.json()["id"])
                    path = f"/internal/v1/ai/incidents/{incident_id}/context"
                    assert (await client.get(path)).status_code == 401
                    for caller, scope in [("other", "context"), ("ai-backend", "results")]:
                        forbidden = await client.get(
                            path,
                            headers={
                                "Authorization": (
                                    f"Bearer fake-service-token:{caller}:main-ai-sink:"
                                    f"incidents.ai.{scope}"
                                ),
                            },
                        )
                        assert forbidden.status_code == 403, forbidden.text
                    response = await client.get(
                        path,
                        headers={
                            "Authorization": (
                                "Bearer fake-service-token:ai-backend:main-ai-sink:"
                                "incidents.ai.context"
                            ),
                        },
                    )
                    assert response.status_code == 200, response.text
                    context = response.json()
                    assert context["incident"]["id"] == str(incident_id)
                    assert "version" not in context["incident"]
                    assert set(context["window"]) == {"from", "to"}
                    assert [item["id"] for item in context["services"]] == [str(service_id)]
                    assert len(context["deployments"]) == 1
                    started = await client.post(
                        f"/api/v1/incidents/{incident_id}/investigations",
                        headers={"Idempotency-Key": "start-result"},
                        json={},
                    )
                    assert started.status_code == 202, started.text
                    investigation_id = UUID(started.json()["id"])
                    service = CallbackService(sessions)
                    evidence_ids = [uuid7(), uuid7()]
                    body = AIResultIn(
                        schema_version="1.0",
                        event_id=uuid7(),
                        investigation_id=investigation_id,
                        incident_id=incident_id,
                        status="completed",
                        finished_at=datetime.now(UTC),
                        report={
                            "summary": "Original RCA",
                            "root_cause": "DB pool exhausted",
                            "remediation": [],
                        },
                        hypotheses=[],
                        evidence=[
                            {
                                "id": str(item_id),
                                "title": f"Evidence {index}",
                                "source": {"source_type": "manual"},
                            }
                            for index, item_id in enumerate(evidence_ids)
                        ],
                        timeline=[],
                    )
                    first = await service.result(body)
                    second = await service.result(
                        body.model_copy(
                            update={"event_id": uuid7(), "status": "failed", "report": None}
                        )
                    )
                    assert first.accepted
                    assert not second.accepted and second.deduplicated
                    evidence_page = await client.get(
                        f"/api/v1/investigations/{investigation_id}/evidence",
                        params={"limit": 1},
                    )
                    assert evidence_page.status_code == 200, evidence_page.text
                    assert len(evidence_page.json()["items"]) == 1
                    assert evidence_page.json()["next_cursor"] is not None
                    remaining = await client.get(
                        f"/api/v1/investigations/{investigation_id}/evidence",
                        params={"limit": 1, "cursor": evidence_page.json()["next_cursor"]},
                    )
                    assert remaining.status_code == 200, remaining.text
                    assert len(remaining.json()["items"]) == 1
                    assert remaining.json()["next_cursor"] is None
                    assert {
                        evidence_page.json()["items"][0]["id"],
                        remaining.json()["items"][0]["id"],
                    } == {str(value) for value in evidence_ids}
                    review = await client.post(
                        f"/api/v1/incidents/{incident_id}/reviews",
                        json={
                            "investigation_id": str(investigation_id),
                            "decision": "corrected",
                            "corrected_root_cause": "Different diagnosis",
                            "comment": "Human review",
                        },
                    )
                    assert review.status_code == 201, review.text
                    second_review = await client.post(
                        f"/api/v1/incidents/{incident_id}/reviews",
                        json={
                            "investigation_id": str(investigation_id),
                            "decision": "confirmed",
                            "comment": "Second reviewer",
                        },
                    )
                    assert second_review.status_code == 201, second_review.text
                    review_page = await client.get(
                        f"/api/v1/incidents/{incident_id}/reviews", params={"limit": 1}
                    )
                    assert review_page.status_code == 200
                    assert review_page.json()["next_cursor"] is not None
                    review_next = await client.get(
                        f"/api/v1/incidents/{incident_id}/reviews",
                        params={"limit": 1, "cursor": review_page.json()["next_cursor"]},
                    )
                    assert review_next.status_code == 200
                    assert (
                        review_next.json()["items"][0]["id"] != review_page.json()["items"][0]["id"]
                    )
                    async with sessions() as session:
                        repo = CallbackRepository(session)
                        result = await repo.result(investigation_id)
                        report = await repo.report(investigation_id)
                        assert result is not None and result.status == "completed"
                        assert report is not None and report.summary == "Original RCA"
                        messages = (
                            await session.execute(select(outbox.c.body, outbox.c.destination))
                        ).all()
                        live_updates = [
                            body
                            for body, destination in messages
                            if body["type"] == "incident.updated"
                            and destination["exchange"] == "incidents.live"
                            and body["data"]["incident"]["id"] == str(incident_id)
                            and body["data"]["incident"]["status"] == "awaiting_review"
                        ]
                        assert len(live_updates) == 1
            finally:
                await transaction.rollback()
    finally:
        await engine.dispose()
