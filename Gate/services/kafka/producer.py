from aiokafka import AIOKafkaProducer

from settings import logger, kafka_config

producer: AIOKafkaProducer | None = None


async def init_producer(bootstrap_servers: str = kafka_config.KAFKA_BROKER):
    try:
        global producer
        producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
        await producer.start()
        logger.info(
            "Kafka producer started successfully",
            extra={"tags": {"service": "gate"}},
        )
    except Exception as e:
        logger.exception(
            f"Failed to start Kafka producer: {e}",
            extra={"tags": {"service": "gate"}},
        )
        raise


async def close_producer():
    global producer
    try:
        await producer.stop()
        logger.info(
            "Kafka producer stopped successfully",
            extra={"tags": {"service": "gate"}},
        )
    except Exception as e:
        logger.exception(
            f"Failed to stop Kafka producer: {e}",
            extra={"tags": {"service": "gate"}},
        )


async def get_kafka_producer() -> AIOKafkaProducer:
    if not producer:
        raise RuntimeError("Kafka producer is not initialized")
    return producer
