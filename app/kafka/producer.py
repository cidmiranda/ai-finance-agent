import json
import logging

from aiokafka import AIOKafkaProducer

from app.config.settings import KAFKA_BOOTSTRAP_SERVERS

logger = logging.getLogger(__name__)

_producer: AIOKafkaProducer | None = None


async def start():
    global _producer
    _producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode(),
    )
    await _producer.start()
    logger.info("Kafka producer started on %s", KAFKA_BOOTSTRAP_SERVERS)


async def stop():
    global _producer
    if _producer:
        await _producer.stop()
        _producer = None


async def publish(topic: str, payload: dict):
    if _producer is None:
        logger.warning("Kafka producer not started, skipping publish to %s", topic)
        return
    await _producer.send_and_wait(topic, payload)
    logger.info("Published to %s: %s", topic, payload)
