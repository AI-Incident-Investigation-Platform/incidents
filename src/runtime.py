import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import aio_pika
import httpx
import uvicorn
from aiip_kit.access.checker import DragonflyAccessChecker
from aiip_kit.access.source import HttpAclSource
from aiip_kit.api_tokens.reader import DragonflyApiTokenReader
from aiip_kit.errors import DependencyUnavailableError, TransientError
from aiip_kit.messaging.consumer import Binding, Consumer, QueueSpec
from aiip_kit.messaging.publisher import AmqpPublisher
from aiip_kit.messaging.topology import events_exchange
from aiip_kit.outbox.relay import OutboxRelay
from aiip_kit.service_auth.jwks import JwksKeys
from aiip_kit.service_auth.provider import AuthServiceTokenProvider
from aiip_kit.service_auth.verifier import JwtServiceTokenVerifier
from fastapi import FastAPI
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.api.ai_callbacks.service import CallbackService
from src.api.incidents.service import IncidentService
from src.api.investigations.service import InvestigationService
from src.api.receipts.service import ReceiptService
from src.api.reports.service import ReportService
from src.core.ai_client import HttpAiClient
from src.core.config_source import HttpConfigSource
from src.core.db.models import outbox
from src.core.projects_topology import ProjectsTopology
from src.core.user_auth import JwtUserAuthenticator
from src.main import create_app
from src.worker.alerts.consumer import start_alert_consumer
from src.worker.alerts.service import AlertWorker
from src.worker.cleanup import CleanupWorker
from src.worker.config import ConfigWorker
from src.worker.dispatch import DispatchWorker


class Config(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")

    database_url: str = Field(min_length=1)
    rabbitmq_url: str = Field(min_length=1)
    cache_url: str = Field(min_length=1)
    auth_url: str = "http://auth.apps:8001"
    projects_url: str = "http://projects.apps:8003"
    ai_backend_url: str = "http://ai-backend.apps:8000"
    auth_client_id: str = "incidents"
    auth_client_secret: str = Field(min_length=1)
    port: int = 8006
    bind_host: str = "0.0.0.0"


cfg = Config()  # type: ignore[call-arg]  # BaseSettings loads required values from the environment.
engine = create_async_engine(cfg.database_url, pool_size=3, max_overflow=0, pool_timeout=0.5)
sessions = async_sessionmaker(engine, expire_on_commit=False)
http = httpx.AsyncClient(timeout=2.0)
redis = Redis.from_url(cfg.cache_url, decode_responses=True)
keys = JwksKeys(http, f"{cfg.auth_url.rstrip('/')}/.well-known/jwks.json")
authenticator = JwtUserAuthenticator(keys)
service_tokens = AuthServiceTokenProvider(
    http, cfg.auth_url, cfg.auth_client_id, cfg.auth_client_secret
)
acl_source = HttpAclSource(
    http,
    cfg.projects_url,
    lambda: service_tokens.token("projects", ["projects.acl.read"]),
)
access = DragonflyAccessChecker(redis, acl_source)
publisher = AmqpPublisher(cfg.rabbitmq_url)
relay = OutboxRelay(sessions, outbox, publisher)
stop = asyncio.Event()
ai_client = HttpAiClient(
    http,
    cfg.ai_backend_url,
    lambda action: service_tokens.token("ai-backend", [f"ai.investigations.{action}"]),
)
dispatch = DispatchWorker(sessions, ai_client)
token_reader = DragonflyApiTokenReader(
    redis,
    http,
    cfg.projects_url,
    lambda: service_tokens.token("projects", ["projects.tokens.read"]),
)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    connection = await aio_pika.connect_robust(cfg.rabbitmq_url)
    channel = await connection.channel(publisher_confirms=True)
    await start_alert_consumer(channel, AlertWorker(sessions))
    config_source = HttpConfigSource(
        http,
        cfg.projects_url,
        lambda: service_tokens.token("projects", ["projects.config.read"]),
    )
    config_worker = ConfigWorker(sessions, config_source)
    cleanup_worker = CleanupWorker(sessions)

    async def project_event(message, attempt):
        if message.type == "config.changed":
            await config_worker.handle(message, attempt)
        elif message.type == "project.deleting":
            await cleanup_worker.handle(message, attempt)
        else:
            raise ValueError("Unknown project event")

    await Consumer(
        channel,
        QueueSpec(
            name="incidents.projects-events",
            bindings=[
                Binding(events_exchange("projects"), "topic", "config.changed"),
                Binding(events_exchange("projects"), "topic", "project.deleting"),
            ],
            transient=(TransientError, DependencyUnavailableError, SQLAlchemyError),
        ),
        project_event,
    ).start()
    task = asyncio.create_task(relay.run(stop))
    dispatch_task = asyncio.create_task(dispatch.run(stop))
    try:
        yield
    finally:
        stop.set()
        await task
        await dispatch_task
        await connection.close()
        await publisher.close()
        await http.aclose()
        await redis.aclose()
        await engine.dispose()


app = create_app(
    IncidentService(sessions, access, topology=ProjectsTopology(http, cfg.projects_url)),
    authenticator.authenticate,
    InvestigationService(sessions, access),
    ReceiptService(sessions, access, token_reader, authenticator.authenticate),
    CallbackService(sessions),
    JwtServiceTokenVerifier(keys, "main-ai-sink"),
    ReportService(sessions, access),
)
app.router.lifespan_context = lifespan


@app.get("/ready")
async def ready() -> dict[str, str]:
    async with engine.connect() as connection:
        await connection.execute(text("SELECT 1"))
    await redis.ping()
    return {"status": "ok"}


def main() -> None:
    uvicorn.run("src.runtime:app", host=cfg.bind_host, port=cfg.port, workers=1)


if __name__ == "__main__":
    main()
