import aio_pika
from aiip_kit.errors import TransientError
from aiip_kit.messaging.consumer import (
    DEAD_TTL_DAYS,
    RETRY_DELAYS_SECONDS,
    Binding,
    Consumer,
    QueueSpec,
    retry_queue,
)
from aiip_kit.messaging.topology import commands_exchange
from aio_pika.abc import AbstractChannel, AbstractQueue
from sqlalchemy.exc import SQLAlchemyError

from src.worker.alerts.service import AlertWorker

ALERTS_QUEUE = "incidents.alerts"
ALERT_MAX_AGE_MS = 15 * 60 * 1000


async def start_alert_consumer(channel: AbstractChannel, worker: AlertWorker) -> AbstractQueue:
    spec = QueueSpec(
        name=ALERTS_QUEUE,
        bindings=[Binding(commands_exchange("incidents"), "direct", "alert.received")],
        max_length=1000,
        max_bytes=16 * 1024 * 1024,
        prefetch=8,
        transient=(TransientError, SQLAlchemyError),
    )
    consumer = Consumer(channel, spec, worker.handle)
    await channel.set_qos(prefetch_count=spec.prefetch)
    await channel.declare_queue(
        f"{ALERTS_QUEUE}.dead",
        durable=True,
        arguments={"x-queue-type": "quorum", "x-message-ttl": DEAD_TTL_DAYS * 86_400_000},
    )
    for delay in RETRY_DELAYS_SECONDS:
        await channel.declare_queue(
            retry_queue(ALERTS_QUEUE, delay),
            durable=True,
            arguments={
                "x-queue-type": "quorum",
                "x-message-ttl": delay * 1000,
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": ALERTS_QUEUE,
            },
        )
    queue = await channel.declare_queue(
        ALERTS_QUEUE,
        durable=True,
        arguments={
            "x-queue-type": "quorum",
            "x-max-length": spec.max_length,
            "x-max-length-bytes": spec.max_bytes,
            "x-message-ttl": ALERT_MAX_AGE_MS,
            "x-overflow": "reject-publish",
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": f"{ALERTS_QUEUE}.dead",
            "x-dead-letter-strategy": "at-least-once",
        },
    )
    exchange = await channel.declare_exchange(
        commands_exchange("incidents"), aio_pika.ExchangeType.DIRECT, durable=True
    )
    await queue.bind(exchange, routing_key="alert.received")
    await queue.consume(consumer.handle)
    return queue
