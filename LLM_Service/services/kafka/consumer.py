import asyncio
import json

from aiokafka import AIOKafkaConsumer

from settings import logger, kafka_config, prometheus_metrics, SERVICE_NAME
from services.llm.generator import generate_response

consumer: AIOKafkaConsumer | None = None


async def init_consumer(
    loop: asyncio.AbstractEventLoop,
    bootstrap_servers: str = kafka_config.KAFKA_BROKER,
    topic: str = kafka_config.KAFKA_TOPIC_REQUESTS,
    client_id: str = kafka_config.CLIENT_ID,
):
    global consumer
    try:
        consumer = AIOKafkaConsumer(
            topic,
            loop=loop,
            client_id=client_id,
            bootstrap_servers=bootstrap_servers,
            enable_auto_commit=False,
        )
        asyncio.create_task(consume_llm_process())
        logger.info(
            "Kafka consumer started successfully",
            extra={"tags": {"service": SERVICE_NAME}},
        )
    except Exception as e:
        logger.exception(
            f"Failed to start Kafka consumer: {e}",
            extra={"tags": {"service": SERVICE_NAME}},
        )
        raise


async def close_consumer():
    try:
        if consumer:
            await consumer.stop()
            logger.info(
                "Kafka consumer stopped successfully",
                extra={"tags": {"service": SERVICE_NAME}},
            )
    except Exception as e:
        logger.exception(
            f"Failed to stop Kafka consumer: {e}",
            extra={"tags": {"service": SERVICE_NAME}},
        )


async def consume_llm_process():
    await consumer.start()
    try:
        async for msg in consumer:
            prometheus_metrics.kafka_messages_received.inc()
            message = json.loads(msg.value.decode("utf-8"))
            logger.info(f"Received message from Kafka topic 'llm_prompts': {message}", extra={"tags": {"service": SERVICE_NAME}})

            if not (prompt := message.get("prompt")):
                logger.warning(f"Invalid message format: {message}", extra={"tags": {"service": SERVICE_NAME}})
                continue

            await generate_response(
                request_id=message.get("request_id"),
                system=message.get("system_context"),
                user=prompt
            )

    except Exception as e:
        logger.error(f"Error while processing Kafka messages: {e}", extra={"tags": {"service": SERVICE_NAME}})
    finally:
        await consumer.stop()
