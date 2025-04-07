import asyncio
import json

from aiokafka import AIOKafkaConsumer

from settings import logger, kafka_config, prometheus_metrics
from services.redis.response import update_response

consumer: AIOKafkaConsumer | None = None


async def init_consumer(
    loop: asyncio.AbstractEventLoop,
    bootstrap_servers: str = kafka_config.KAFKA_BROKER,
    topic: str = kafka_config.KAFKA_TOPIC_RESPONSES,
):
    global consumer
    try:
        consumer = AIOKafkaConsumer(
            topic,
            loop=loop,
            bootstrap_servers=bootstrap_servers,
            enable_auto_commit=False,
        )
        asyncio.create_task(consume_llm_responses())
        logger.info(
            "Kafka consumer started successfully",
            extra={"tags": {"service": "gate"}},
        )
    except Exception as e:
        logger.exception(
            f"Failed to start Kafka consumer: {e}",
            extra={"tags": {"service": "gate"}},
        )
        raise


async def close_consumer():
    global consumer
    try:
        if consumer:
            await consumer.stop()
            logger.info(
                "Kafka consumer stopped successfully",
                extra={"tags": {"service": "gate"}},
            )
    except Exception as e:
        logger.exception(
            f"Failed to stop Kafka consumer: {e}",
            extra={"tags": {"service": "gate"}},
        )


async def consume_llm_responses():
    global consumer
    await consumer.start()
    try:
        async for msg in consumer:
            prometheus_metrics.KAFKA_MESSAGES_CONSUMED.labels("llm_responses").inc()
            message = json.loads(msg.value.decode("utf-8"))
            logger.info(
                f"Received message from Kafka topic 'llm_responses': {message}",
                extra={"tags": {"service": "gate"}},
            )

            request_id = message.get("request_id")
            result = message.get("result")
            status = message.get("status")

            # Если запрос еще актуален, обновляем результат и устанавливаем событие
            if await update_response(request_id, result, status):
                logger.info(
                    f"Updated response_store for request_id={request_id}: result={result}, status={status}",
                    extra={"tags": {"service": "gate"}},
                )
            else:
                # Если запрос уже завершен (таймаут или удаление), логируем сообщение
                logger.warning(
                    f"Received delayed response for request_id={request_id}. Message ignored. Content: {message}",
                    extra={"tags": {"service": "gate"}},
                )

    except Exception as e:
        logger.error(
            f"Error while processing Kafka messages: {e}",
            extra={"tags": {"service": "gate"}},
        )
    finally:
        await consumer.stop()


async def get_kafka_consumer() -> AIOKafkaConsumer:
    if not consumer:
        raise RuntimeError("Kafka consumer is not initialized")
    return consumer
