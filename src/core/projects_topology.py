from typing import Literal
from uuid import UUID

import httpx
from aiip_kit.errors import DependencyUnavailableError
from pydantic import BaseModel


class EnvironmentRef(BaseModel):
    id: UUID


class EnvironmentPage(BaseModel):
    items: list[EnvironmentRef]
    next_cursor: str | None


class ServiceHealth(BaseModel):
    service_id: UUID
    status: Literal["healthy", "degraded", "critical", "unknown"]


class Topology(BaseModel):
    services: list[ServiceHealth]


class ProjectsTopology:
    def __init__(self, client: httpx.AsyncClient, projects_url: str) -> None:
        self._client = client
        self._url = projects_url.rstrip("/")

    async def counts(
        self, project_id: UUID, environment_id: UUID | None, authorization: str
    ) -> tuple[int, int]:
        headers = {"Authorization": authorization}
        base = f"{self._url}/api/v1/projects/{project_id}"
        try:
            environments = [environment_id] if environment_id is not None else []
            if environment_id is None:
                params: dict[str, str] = {"limit": "100"}
                seen: set[str] = set()
                while True:
                    response = await self._client.get(
                        f"{base}/environments", params=params, headers=headers
                    )
                    response.raise_for_status()
                    page = EnvironmentPage.model_validate(response.json())
                    environments.extend(item.id for item in page.items)
                    if page.next_cursor is None:
                        break
                    if page.next_cursor in seen or len(seen) >= 99:
                        raise DependencyUnavailableError("projects")
                    seen.add(page.next_cursor)
                    params["cursor"] = page.next_cursor
            health: dict[UUID, str] = {}
            for environment in dict.fromkeys(environments):
                response = await self._client.get(
                    f"{base}/topology", params={"environment_id": str(environment)}, headers=headers
                )
                response.raise_for_status()
                topology = Topology.model_validate(response.json())
                health.update((item.service_id, item.status) for item in topology.services)
            return sum(status == "healthy" for status in health.values()), len(health)
        except (httpx.HTTPError, ValueError) as exc:
            raise DependencyUnavailableError("projects") from exc
