from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Header

from src.api.receipts.service import ReceiptService, ReceiptView


def build_router(service: ReceiptService) -> APIRouter:
    router = APIRouter(prefix="/api/v1/ingest", tags=["ingest receipts"])

    @router.get("/receipts/{receipt_id}")
    async def get(
        receipt_id: UUID,
        authorization: Annotated[str | None, Header()] = None,
        x_ingest_key: Annotated[str | None, Header(alias="X-Ingest-Key")] = None,
    ) -> ReceiptView:
        return await service.get(receipt_id, authorization, x_ingest_key)

    return router
