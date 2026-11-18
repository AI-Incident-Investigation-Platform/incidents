from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Annotated
from uuid import UUID

from aiip_kit.access.model import Principal
from fastapi import APIRouter, Cookie, Depends, Header, Query, status
from pydantic import AwareDatetime

from src.api.incidents.schemas import (
    IncidentCreate,
    IncidentPage,
    IncidentPatch,
    IncidentStatusChange,
    IncidentView,
    Overview,
    ReviewCreate,
    ReviewPage,
    ReviewView,
    TimelinePage,
)
from src.api.incidents.service import IncidentService

Authenticate = Callable[[str | None], Awaitable[Principal]]


def build_router(service: IncidentService, authenticate: Authenticate) -> APIRouter:
    router = APIRouter(prefix="/api/v1", tags=["incidents"])

    async def principal(authorization: Annotated[str | None, Header()] = None) -> Principal:
        return await authenticate(authorization)

    @router.get("/overview")
    async def overview(
        project_id: UUID,
        environment_id: UUID | None = None,
        start: Annotated[AwareDatetime | None, Query(alias="from")] = None,
        end: Annotated[AwareDatetime | None, Query(alias="to")] = None,
        authorization: Annotated[str | None, Header()] = None,
        access_token: Annotated[str | None, Cookie()] = None,
    ) -> Overview:
        auth = authorization or (f"Bearer {access_token}" if access_token else None)
        user = await authenticate(auth)
        return await service.overview(user, auth or "", project_id, environment_id, start, end)

    @router.post("/incidents", status_code=status.HTTP_201_CREATED)
    async def create(
        body: IncidentCreate, user: Annotated[Principal, Depends(principal)]
    ) -> IncidentView:
        return await service.create(user, body)

    @router.get("/incidents")
    async def list_incidents(
        project_id: UUID,
        user: Annotated[Principal, Depends(principal)],
        limit: Annotated[int, Query(ge=1, le=100)] = 50,
        cursor: str | None = None,
        status_filter: Annotated[str | None, Query(alias="status")] = None,
        environment_id: UUID | None = None,
        service_id: UUID | None = None,
        severity: str | None = None,
        search: str | None = None,
        from_time: Annotated[datetime | None, Query(alias="from")] = None,
        to_time: Annotated[datetime | None, Query(alias="to")] = None,
        sort: str = "detected_at_desc",
    ) -> IncidentPage:
        return await service.list(
            user,
            project_id,
            limit=limit,
            cursor=cursor,
            status=status_filter,
            environment_id=environment_id,
            service_id=service_id,
            severity=severity,
            search=search,
            from_time=from_time,
            to_time=to_time,
            sort=sort,
        )

    @router.get("/incidents/{incident_id}")
    async def get(
        incident_id: UUID, user: Annotated[Principal, Depends(principal)]
    ) -> IncidentView:
        return await service.get(user, incident_id)

    @router.patch("/incidents/{incident_id}")
    async def patch(
        incident_id: UUID,
        body: IncidentPatch,
        user: Annotated[Principal, Depends(principal)],
        if_match: Annotated[str, Header(alias="If-Match")],
    ) -> IncidentView:
        version = int(if_match.strip('"'))
        return await service.patch(user, incident_id, body, version)

    @router.patch("/incidents/{incident_id}/status")
    async def change_status(
        incident_id: UUID,
        body: IncidentStatusChange,
        user: Annotated[Principal, Depends(principal)],
    ) -> IncidentView:
        return await service.change_status(user, incident_id, body)

    @router.get("/incidents/{incident_id}/timeline")
    async def timeline(
        incident_id: UUID,
        user: Annotated[Principal, Depends(principal)],
        limit: Annotated[int, Query(ge=1, le=100)] = 50,
        cursor: str | None = None,
    ) -> TimelinePage:
        return await service.timeline(user, incident_id, limit=limit, cursor=cursor)

    @router.post("/incidents/{incident_id}/reviews", status_code=status.HTTP_201_CREATED)
    async def add_review(
        incident_id: UUID,
        body: ReviewCreate,
        user: Annotated[Principal, Depends(principal)],
    ) -> ReviewView:
        return await service.add_review(user, incident_id, body)

    @router.get("/incidents/{incident_id}/reviews")
    async def reviews(
        incident_id: UUID,
        user: Annotated[Principal, Depends(principal)],
        limit: Annotated[int, Query(ge=1, le=100)] = 50,
        cursor: str | None = None,
    ) -> ReviewPage:
        return await service.reviews(user, incident_id, limit=limit, cursor=cursor)

    return router
