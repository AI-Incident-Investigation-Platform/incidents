from collections.abc import Awaitable, Callable
from typing import Annotated
from uuid import UUID

from aiip_kit.access.model import Principal
from fastapi import APIRouter, Depends, Header, Query, status

from src.api.investigations.schemas import (
    CancelAck,
    InvestigationCreate,
    InvestigationsPage,
    InvestigationView,
)
from src.api.investigations.service import InvestigationService

Authenticate = Callable[[str | None], Awaitable[Principal]]


def build_router(service: InvestigationService, authenticate: Authenticate) -> APIRouter:
    router = APIRouter(prefix="/api/v1", tags=["investigations"])

    async def principal(authorization: Annotated[str | None, Header()] = None) -> Principal:
        return await authenticate(authorization)

    @router.post("/incidents/{incident_id}/investigations", status_code=status.HTTP_202_ACCEPTED)
    async def start(
        incident_id: UUID,
        body: InvestigationCreate,
        user: Annotated[Principal, Depends(principal)],
        idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> InvestigationView:
        return await service.start(user, incident_id, body, idempotency_key)

    @router.get("/incidents/{incident_id}/investigations")
    async def list_for_incident(
        incident_id: UUID, user: Annotated[Principal, Depends(principal)]
    ) -> InvestigationsPage:
        return await service.list_for_incident(user, incident_id)

    @router.get("/investigations")
    async def list_for_project(
        project_id: UUID,
        user: Annotated[Principal, Depends(principal)],
        status_filter: Annotated[str | None, Query(alias="status")] = None,
        cursor: str | None = None,
        limit: Annotated[int, Query(ge=1, le=100)] = 50,
    ) -> InvestigationsPage:
        return await service.list_for_project(
            user, project_id, status=status_filter, cursor=cursor, limit=limit
        )

    @router.get("/investigations/{investigation_id}")
    async def get(
        investigation_id: UUID, user: Annotated[Principal, Depends(principal)]
    ) -> InvestigationView:
        return await service.get(user, investigation_id)

    @router.post("/investigations/{investigation_id}/cancel", status_code=202)
    async def cancel(
        investigation_id: UUID, user: Annotated[Principal, Depends(principal)]
    ) -> CancelAck:
        return await service.cancel(user, investigation_id)

    @router.post("/investigations/{investigation_id}/retry", status_code=202)
    async def retry(
        investigation_id: UUID,
        user: Annotated[Principal, Depends(principal)],
        idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
    ) -> InvestigationView:
        return await service.retry(user, investigation_id, idempotency_key)

    return router
