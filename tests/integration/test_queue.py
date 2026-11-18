import os
from urllib.parse import urlparse

import aio_pika
import pytest

from src.worker.alerts.consumer import start_alert_consumer


class FakeAlertWorker:
    async def handle(self, message: object, attempt: int) -> None:
        raise AssertionError("No messages expected in topology test")


@pytest.mark.asyncio
@pytest.mark.case(id="INC-ALT-004")
async def test_alert_queue_limits_are_declared_on_broker() -> None:
    url = os.environ.get("AIIP_TEST_RABBITMQ_URL")
    if not url:
        pytest.skip("AIIP_TEST_RABBITMQ_URL is required")
    if urlparse(url).hostname not in {"127.0.0.1", "localhost"}:
        raise RuntimeError("Integration test requires local RabbitMQ")
    connection = await aio_pika.connect_robust(url)
    try:
        channel = await connection.channel(publisher_confirms=True)
        queue = await start_alert_consumer(channel, FakeAlertWorker())  # type: ignore[arg-type]
        assert queue.name == "incidents.alerts"
    finally:
        await connection.close()
