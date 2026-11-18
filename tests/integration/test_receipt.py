import os
from datetime import UTC, datetime
from urllib.parse import urlparse
from uuid import uuid7

import pytest
from aiip_kit.access.checker import FakeAccessChecker
from aiip_kit.access.model import Principal
from aiip_kit.api_tokens.reader import ApiTokenInfo, FakeApiTokenReader
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.api.receipts.service import ReceiptNotFound, ReceiptService
from src.core.db.models import IngestReceipt


@pytest.mark.asyncio
@pytest.mark.case(id="INC-ALT-005")
async def test_receipt_key_can_only_read_its_project() -> None:
    url = os.environ.get("AIIP_TEST_DATABASE_URL")
    if not url:
        pytest.skip("AIIP_TEST_DATABASE_URL is required")
    if not urlparse(url).path.lstrip("/").startswith("incidents_test"):
        raise RuntimeError("Integration test requires an incidents_test database")
    engine = create_async_engine(url)
    project_id, other_project, token_id, receipt_id = (uuid7() for _ in range(4))
    token = ApiTokenInfo(
        state="active",
        token_id=token_id,
        project_id=project_id,
        environment_id=uuid7(),
        scopes=frozenset({"alert.ingest"}),
        lifecycle_version=1,
        ver=1,
        expires_at=None,
        observed_at_ms=0,
        fresh_until_ms=0,
    )
    other_token = ApiTokenInfo(
        state="active",
        token_id=uuid7(),
        project_id=other_project,
        environment_id=uuid7(),
        scopes=frozenset({"alert.ingest"}),
        lifecycle_version=1,
        ver=1,
        expires_at=None,
        observed_at_ms=0,
        fresh_until_ms=0,
    )

    async def authenticate(authorization: str | None) -> Principal:
        return Principal(user_id=uuid7(), session_id=uuid7())

    try:
        async with engine.connect() as connection:
            transaction = await connection.begin()
            try:
                sessions = async_sessionmaker(
                    bind=connection, join_transaction_mode="create_savepoint"
                )
                async with sessions.begin() as session:
                    session.add(
                        IngestReceipt(
                            id=receipt_id,
                            project_id=project_id,
                            token_id=token_id,
                            kind="alert",
                            status="accepted",
                            received_at=datetime.now(UTC),
                        )
                    )
                service = ReceiptService(
                    sessions,
                    FakeAccessChecker(),
                    FakeApiTokenReader({"ok": token, "other": other_token}),
                    authenticate,
                )
                view = await service.get(receipt_id, None, "ok")
                assert view.status == "accepted"
                with pytest.raises(ReceiptNotFound):
                    await service.get(receipt_id, None, "other")
            finally:
                await transaction.rollback()
    finally:
        await engine.dispose()
