from collections.abc import Awaitable, Callable
from typing import Protocol
from uuid import UUID

import httpx


class AiBusy(Exception):
    def __init__(self, retry_after: int = 5) -> None:
        self.retry_after = max(1, min(retry_after, 300))


class AiUncertain(Exception):
    pass


class AiClient(Protocol):
    async def start(self, request: dict) -> str: ...

    async def readback(self, investigation_id: UUID) -> str | None: ...

    async def cancel(self, investigation_id: UUID) -> None: ...


class HttpAiClient:
    def __init__(
        self,
        client: httpx.AsyncClient,
        base_url: str,
        token: Callable[[str], Awaitable[str]],
    ) -> None:
        self._client = client
        self._url = base_url.rstrip("/")
        self._token = token

    async def start(self, request: dict) -> str:
        try:
            response = await self._client.post(
                f"{self._url}/internal/v1/investigations",
                json=request,
                headers={
                    "Authorization": f"Bearer {await self._token('start')}",
                    "Idempotency-Key": str(request["investigation_id"]),
                },
                timeout=5.0,
            )
        except httpx.HTTPError as exc:
            raise AiUncertain from exc
        if response.status_code == 429:
            try:
                retry_after = int(response.headers.get("Retry-After", "5"))
            except ValueError:
                retry_after = 5
            raise AiBusy(retry_after)
        if response.status_code != 202:
            raise AiUncertain
        try:
            body = response.json()
            if body["investigation_id"] != request["investigation_id"]:
                raise ValueError("AI returned another investigation_id")
            if body["status"] not in {"pending", "running"}:
                raise ValueError("Invalid AI status")
            return body["status"]
        except (ValueError, KeyError, TypeError) as exc:
            raise AiUncertain from exc

    async def readback(self, investigation_id: UUID) -> str | None:
        try:
            response = await self._client.get(
                f"{self._url}/internal/v1/investigations/{investigation_id}",
                headers={"Authorization": f"Bearer {await self._token('readback')}"},
                timeout=5.0,
            )
        except httpx.HTTPError as exc:
            raise AiUncertain from exc
        if response.status_code == 404:
            return None
        if response.status_code != 200:
            raise AiUncertain
        try:
            body = response.json()
            if body["id"] != str(investigation_id):
                raise ValueError("AI returned another investigation_id")
            return str(body["status"])
        except (ValueError, KeyError, TypeError) as exc:
            raise AiUncertain from exc

    async def cancel(self, investigation_id: UUID) -> None:
        try:
            response = await self._client.post(
                f"{self._url}/internal/v1/investigations/{investigation_id}/cancel",
                headers={"Authorization": f"Bearer {await self._token('cancel')}"},
                timeout=5.0,
            )
        except httpx.HTTPError as exc:
            raise AiUncertain from exc
        if response.status_code != 202:
            raise AiUncertain


class FakeAiClient:
    def __init__(self) -> None:
        self.starts: list[dict] = []
        self.runs: dict[UUID, str] = {}
        self.lose_start_response = False
        self.busy = False
        self.cancels: list[UUID] = []

    async def start(self, request: dict) -> str:
        self.starts.append(request)
        if self.busy:
            raise AiBusy
        run_id = UUID(request["investigation_id"])
        self.runs[run_id] = "pending"
        if self.lose_start_response:
            raise AiUncertain
        return "pending"

    async def readback(self, investigation_id: UUID) -> str | None:
        return self.runs.get(investigation_id)

    async def cancel(self, investigation_id: UUID) -> None:
        self.cancels.append(investigation_id)
