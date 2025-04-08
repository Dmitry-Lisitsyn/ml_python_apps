import json
from typing import Any

from settings import prometheus_metrics, logger, SERVICE_NAME, settings, LLMType, kafka_config
from services.kafka.producer import get_kafka_producer
from services.llm.api import giga, ollama


match settings.LLM_MODEL:
    case LLMType.giga:
        LLM = giga.GigaApi()
    case LLMType.ollama:
        LLM = ollama.OllamaApi()
    case _:
        LLM = giga.GigaApi()


async def process_llm_result(request_id: int, status: str, response: Any):
    message = {"request_id": request_id, "status": status, "result": response}
    logger.info(f"Sending result to Kafka topic 'llm_responses': {message}", extra={"tags": {"service": SERVICE_NAME}})
    producer = await get_kafka_producer()
    await producer.send_and_wait(topic=kafka_config.KAFKA_TOPIC_RESPONSES, value=json.dumps(message).encode("utf-8"))


async def generate_response(request_id: str | int, system: str, user: str):
    try:
        response = await LLM.get_llm_response(system, user)
        logger.info(
            f"Generated response for request_id={request_id}: {response}",
            extra={"tags": {"service": SERVICE_NAME}}
        )

        await process_llm_result(request_id, "success", response)
        prometheus_metrics.kafka_messages_processed.inc()

    except Exception as e:
        logger.error(
            f"Error generating response for request_id={request_id}: {e}",
            extra={"tags": {"service": SERVICE_NAME}}
        )
        await process_llm_result(request_id, "error", repr(e))
        prometheus_metrics.kafka_messages_failed.inc()
