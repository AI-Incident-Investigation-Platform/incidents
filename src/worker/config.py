from aiip_kit.messaging.message import MessageV1
from aiip_kit.outbox.inbox import InboxGuard
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.core.config_source import ConfigSource
from src.core.db.alert_repository import AlertRepository
from src.core.db.models import ProjectConfig, inbox


class ConfigWorker:
    def __init__(self, sessions: async_sessionmaker[AsyncSession], source: ConfigSource) -> None:
        self._sessions = sessions
        self._source = source
        self._inbox = InboxGuard(inbox)

    async def handle(self, message: MessageV1, attempt: int) -> None:
        if message.type != "config.changed" or message.project_id is None:
            raise ValueError("Неизвестное событие")
        version = int(message.data.get("version", message.data.get("config_version", 0)))
        if version < 1:
            raise ValueError("В сообщении нет версии конфигурации")
        snapshot = await self._source.fetch(message.project_id, version)
        async with self._sessions.begin() as session:
            if not await self._inbox.accept(session, "config.changed", message):
                return
            repo = AlertRepository(session)
            await repo.lock_project(message.project_id)
            if await repo.tombstone(message.project_id) is not None:
                return
            row = await repo.config_version(message.project_id, version)
            if row is None:
                session.add(
                    ProjectConfig(project_id=message.project_id, version=version, snapshot=snapshot)
                )
            elif row.snapshot != snapshot:
                raise ValueError("Версия конфигурации изменена задним числом")
