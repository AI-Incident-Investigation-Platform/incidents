from aiip_kit.errors import InboxConflictError
from aiip_kit.messaging.message import Actor, MessageV1
from aiip_kit.messaging.topology import Destination
from aiip_kit.outbox.inbox import InboxGuard
from aiip_kit.outbox.writer import OutboxWriter
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.core.db.cleanup_repository import TABLE_FILTERS, CleanupRepository
from src.core.db.models import inbox, outbox


class CleanupWorker:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions
        self._inbox = InboxGuard(inbox)
        self._outbox = OutboxWriter(outbox)

    async def handle(self, message: MessageV1, attempt: int) -> None:
        if message.type != "project.deleting" or message.project_id is None:
            raise ValueError("Неизвестное событие")
        if message.lifecycle_version is None:
            raise ValueError("Нет версии жизненного цикла проекта")
        project_id = message.project_id
        async with self._sessions() as session:
            stored_hash = await session.scalar(
                select(inbox.c.payload_hash).where(
                    inbox.c.handler == "project.deleting",
                    inbox.c.message_id == message.message_id,
                )
            )
        if stored_hash is not None:
            if stored_hash != message.payload_hash():
                raise InboxConflictError("project.deleting: message_id with different payload")
            return
        async with self._sessions.begin() as session:
            await CleanupRepository(session).mark_deleting(project_id, message.lifecycle_version)
        for table, predicate in TABLE_FILTERS:
            while True:
                async with self._sessions.begin() as session:
                    deleted = await CleanupRepository(session).delete_batch(
                        table, predicate, project_id
                    )
                if deleted < 500:
                    break
        async with self._sessions.begin() as session:
            if not await self._inbox.accept(session, "project.deleting", message):
                return
            completed = MessageV1.command(
                "cleanup.completed",
                "incidents",
                {
                    "project_id": str(project_id),
                    "lifecycle_version": message.lifecycle_version,
                    "participant": "incidents",
                },
                project_id=project_id,
                lifecycle_version=message.lifecycle_version,
                actor=Actor(kind="service", id="incidents"),
                correlation_id=message.correlation_id or message.message_id,
                causation_id=message.message_id,
            )
            await self._outbox.add(
                session,
                completed,
                Destination.command("projects", "cleanup.completed"),
            )
