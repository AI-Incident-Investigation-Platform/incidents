from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from uuid import UUID

from aiip_kit.access.checker import AccessChecker
from aiip_kit.access.model import Principal
from aiip_kit.api_tokens.reader import ApiTokenReader
from aiip_kit.errors import NotAuthenticatedError, PermissionDeniedError
from pydantic import BaseModel, ConfigDict
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.core.db.alert_repository import AlertRepository

Authenticate = Callable[[str | None], Awaitable[Principal]]


class ReceiptNotFound(Exception):
    pass


class ReceiptView(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    kind: str
    status: str
    reason: str | None
    resource_id: UUID | None
    incident_id: UUID | None
    received_at: datetime
    processed_at: datetime | None


class ReceiptService:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        access: AccessChecker,
        tokens: ApiTokenReader,
        authenticate: Authenticate,
    ) -> None:
        self._sessions = sessions
        self._access = access
        self._tokens = tokens
        self._authenticate = authenticate

    async def get(
        self, receipt_id: UUID, authorization: str | None, ingest_key: str | None
    ) -> ReceiptView:
        token = None
        principal = None
        if ingest_key:
            token = await self._tokens.resolve(ingest_key)
            if (
                token is None
                or token.state != "active"
                or (token.expires_at is not None and token.expires_at <= datetime.now(UTC))
            ):
                raise NotAuthenticatedError
            if not ({"alert.ingest", "deployment.ingest"} & token.scopes):
                raise PermissionDeniedError("ingest.receipt.read")
        else:
            principal = await self._authenticate(authorization)
        async with self._sessions() as session:
            receipt = await AlertRepository(session).receipt(receipt_id)
            if receipt is None:
                raise ReceiptNotFound
            project_id = receipt.project_id
            view = ReceiptView.model_validate(receipt)
        if token is not None:
            if token.project_id != project_id:
                raise ReceiptNotFound
        elif principal is not None:
            await self._access.require(principal, project_id, "project.read")
        return view
