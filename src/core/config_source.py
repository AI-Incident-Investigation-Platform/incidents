from collections.abc import Awaitable, Callable
from typing import Protocol
from uuid import UUID

import httpx
from aiip_kit.errors import DependencyUnavailableError


class ConfigSource(Protocol):
    async def fetch(self, project_id: UUID, version: int) -> dict: ...


class HttpConfigSource:
    def __init__(
        self,
        client: httpx.AsyncClient,
        projects_url: str,
        token: Callable[[], Awaitable[str]],
    ) -> None:
        self._client = client
        self._url = projects_url.rstrip("/")
        self._token = token

    async def fetch(self, project_id: UUID, version: int) -> dict:
        try:
            response = await self._client.get(
                f"{self._url}/internal/v1/projects/{project_id}/config",
                params={"version": version},
                headers={"Authorization": f"Bearer {await self._token()}"},
                timeout=2.0,
            )
            response.raise_for_status()
            snapshot = response.json()
            if not isinstance(snapshot, dict):
                raise ValueError("Некорректный снимок конфигурации")
            return snapshot
        except (httpx.HTTPError, ValueError, TypeError) as exc:
            raise DependencyUnavailableError("projects") from exc


class FakeConfigSource:
    def __init__(self, snapshots: dict[tuple[UUID, int], dict] | None = None) -> None:
        self.snapshots = snapshots or {}

    async def fetch(self, project_id: UUID, version: int) -> dict:
        return self.snapshots[(project_id, version)]
