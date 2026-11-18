from aiip_kit.errors import (
    DependencyUnavailableError,
    NotAuthenticatedError,
    PermissionDeniedError,
    ProjectNotFoundError,
)
from aiip_kit.service_auth.verifier import ServiceTokenVerifier
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from src.api.ai_callbacks.router import build_router as build_ai_router
from src.api.ai_callbacks.service import CallbackConflict, CallbackService
from src.api.incidents.router import Authenticate, build_router
from src.api.incidents.rules import IncidentConflict
from src.api.incidents.service import IncidentNotFound, IncidentService, StaleIncident
from src.api.investigations.router import build_router as build_investigation_router
from src.api.investigations.rules import InvestigationConflict
from src.api.investigations.service import (
    IdempotencyConflict,
    InvestigationNotFound,
    InvestigationService,
)
from src.api.receipts.router import build_router as build_receipts_router
from src.api.receipts.service import ReceiptNotFound, ReceiptService
from src.api.reports.router import build_internal_router, build_public_router
from src.api.reports.service import ReportNotFound, ReportService


def create_app(
    service: IncidentService,
    authenticate: Authenticate,
    investigations: InvestigationService | None = None,
    receipts: ReceiptService | None = None,
    callbacks: CallbackService | None = None,
    service_verifier: ServiceTokenVerifier | None = None,
    reports: ReportService | None = None,
) -> FastAPI:
    app = FastAPI(title="AIIP incidents")
    app.include_router(build_router(service, authenticate))
    if investigations is not None:
        app.include_router(build_investigation_router(investigations, authenticate))
    if receipts is not None:
        app.include_router(build_receipts_router(receipts))
    if callbacks is not None and service_verifier is not None:
        app.include_router(build_ai_router(callbacks, service_verifier))
    if reports is not None:
        app.include_router(build_public_router(reports, authenticate))
        if service_verifier is not None:
            app.include_router(build_internal_router(reports, service_verifier))

    @app.exception_handler(IncidentNotFound)
    @app.exception_handler(InvestigationNotFound)
    @app.exception_handler(ReceiptNotFound)
    @app.exception_handler(ReportNotFound)
    @app.exception_handler(ProjectNotFoundError)
    async def not_found(request: Request, exc: Exception) -> JSONResponse:
        return JSONResponse({"error": {"code": "not_found"}}, status_code=404)

    @app.exception_handler(IncidentConflict)
    @app.exception_handler(InvestigationConflict)
    @app.exception_handler(IdempotencyConflict)
    @app.exception_handler(CallbackConflict)
    async def conflict(request: Request, exc: IncidentConflict) -> JSONResponse:
        return JSONResponse(
            {"error": {"code": "incident.conflict", "message": str(exc)}}, status_code=409
        )

    @app.exception_handler(StaleIncident)
    async def stale(request: Request, exc: StaleIncident) -> JSONResponse:
        return JSONResponse({"error": {"code": "incident.stale"}}, status_code=412)

    @app.exception_handler(ValueError)
    async def invalid(request: Request, exc: ValueError) -> JSONResponse:
        return JSONResponse(
            {"error": {"code": "invalid_request", "message": str(exc)}}, status_code=422
        )

    @app.exception_handler(NotAuthenticatedError)
    async def unauthenticated(request: Request, exc: NotAuthenticatedError) -> JSONResponse:
        return JSONResponse({"error": {"code": "unauthenticated"}}, status_code=401)

    @app.exception_handler(PermissionDeniedError)
    async def forbidden(request: Request, exc: PermissionDeniedError) -> JSONResponse:
        return JSONResponse({"error": {"code": "permission_denied"}}, status_code=403)

    @app.exception_handler(DependencyUnavailableError)
    async def unavailable(request: Request, exc: DependencyUnavailableError) -> JSONResponse:
        return JSONResponse({"error": {"code": "dependency_unavailable"}}, status_code=503)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app
