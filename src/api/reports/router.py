from collections.abc import Awaitable, Callable
from typing import Annotated
from uuid import UUID

from aiip_kit.access.model import Principal
from aiip_kit.service_auth.verifier import ServiceTokenVerifier
from fastapi import APIRouter, Depends, Header, Query

from src.api.reports.service import ReportService

Authenticate = Callable[[str | None], Awaitable[Principal]]


def build_public_router(service: ReportService, authenticate: Authenticate) -> APIRouter:
    router = APIRouter(prefix="/api/v1", tags=["reports"])

    async def principal(authorization: Annotated[str | None, Header()] = None) -> Principal:
        return await authenticate(authorization)

    @router.get("/investigations/{investigation_id}/report")
    async def report(
        investigation_id: UUID, user: Annotated[Principal, Depends(principal)]
    ) -> dict:
        return await service.report(user, investigation_id)

    @router.get("/investigations/{investigation_id}/events")
    async def events(
        investigation_id: UUID,
        user: Annotated[Principal, Depends(principal)],
        after_sequence: Annotated[int, Query(ge=0)] = 0,
        limit: Annotated[int, Query(ge=1, le=100)] = 50,
    ) -> dict:
        return await service.events(
            user, investigation_id, after_sequence=after_sequence, limit=limit
        )

    @router.get("/investigations/{investigation_id}/hypotheses")
    async def hypotheses(
        investigation_id: UUID, user: Annotated[Principal, Depends(principal)]
    ) -> dict:
        return await service.hypotheses(user, investigation_id)

    @router.get("/investigations/{investigation_id}/evidence")
    async def evidence(
        investigation_id: UUID,
        user: Annotated[Principal, Depends(principal)],
        limit: Annotated[int, Query(ge=1, le=100)] = 50,
        cursor: str | None = None,
    ) -> dict:
        return await service.evidence(user, investigation_id, limit, cursor)

    @router.get("/evidence/{evidence_id}")
    async def evidence_item(
        evidence_id: UUID, user: Annotated[Principal, Depends(principal)]
    ) -> dict:
        return await service.evidence_item(user, evidence_id)

    @router.get("/incidents/{incident_id}/alerts")
    async def alerts(
        incident_id: UUID,
        user: Annotated[Principal, Depends(principal)],
        limit: Annotated[int, Query(ge=1, le=100)] = 50,
        cursor: str | None = None,
    ) -> dict:
        return await service.alerts(user, incident_id, limit, cursor)

    return router


def build_internal_router(service: ReportService, verifier: ServiceTokenVerifier) -> APIRouter:
    router = APIRouter(prefix="/internal/v1", tags=["internal reports"])

    @router.get("/investigations/{investigation_id}/report")
    async def report(
        investigation_id: UUID,
        authorization: Annotated[str | None, Header()] = None,
    ) -> dict:
        await verifier.verify(authorization, "incidents.reports.read")
        return await service.internal_report(investigation_id)

    return router
