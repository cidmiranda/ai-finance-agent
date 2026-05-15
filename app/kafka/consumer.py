import asyncio
import json
import logging
from collections.abc import Awaitable, Callable

from aiokafka import AIOKafkaConsumer

from app.config.settings import KAFKA_BOOTSTRAP_SERVERS
from app.kafka.topics import RECONCILIATION_REQUESTED

logger = logging.getLogger(__name__)

Handler = Callable[[dict], Awaitable[None]]


async def start(handler: Handler):
    consumer = AIOKafkaConsumer(
        RECONCILIATION_REQUESTED,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="finance-agent",
        value_deserializer=lambda v: json.loads(v.decode()),
        auto_offset_reset="earliest",
    )

    await consumer.start()
    logger.info(
        "Kafka consumer started, listening on '%s'", RECONCILIATION_REQUESTED
    )

    try:
        async for msg in consumer:
            asyncio.create_task(handler(msg.value))
    finally:
        await consumer.stop()
