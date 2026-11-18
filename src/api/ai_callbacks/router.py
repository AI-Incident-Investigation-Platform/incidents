from typing import Annotated
from uuid import UUID

from aiip_kit.errors import PermissionDeniedError
from aiip_kit.service_auth.verifier import ServiceTokenVerifier
from fastapi import APIRouter, Header, status

from src.api.ai_callbacks.schemas import (
    AIResultIn,
    CallbackAck,
    IncidentContext,
    InvestigationEventIn,
)
from src.api.ai_callbacks.service import CallbackService


def build_router(service: CallbackService, verifier: ServiceTokenVerifier) -> APIRouter:
    router = APIRouter(prefix="/internal/v1/ai", tags=["AI callbacks"])

    @router.get("/incidents/{incident_id}/context", response_model_exclude_none=True)
    async def context(
        incident_id: UUID,
        authorization: Annotated[str | None, Header()] = None,
    ) -> IncidentContext:
        identity = await verifier.verify(authorization, "incidents.ai.context")
        if identity.service != "ai-backend":
            raise PermissionDeniedError("incidents.ai.context")
        return await service.context(incident_id)

    @router.post("/events", status_code=status.HTTP_202_ACCEPTED)
    async def event(
        body: InvestigationEventIn,
        authorization: Annotated[str | None, Header()] = None,
    ) -> CallbackAck:
        await verifier.verify(authorization, "incidents.ai.events")
        return await service.event(body)

    @router.post("/results", status_code=status.HTTP_202_ACCEPTED)
    async def result(
        body: AIResultIn,
        authorization: Annotated[str | None, Header()] = None,
    ) -> CallbackAck:
        await verifier.verify(authorization, "incidents.ai.results")
        return await service.result(body)

    return router
