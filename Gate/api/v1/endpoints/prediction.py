import json

from aiokafka import AIOKafkaProducer
from fastapi import APIRouter, Request, Depends

from settings import logger, prometheus_metrics
from services.kafka import get_kafka_producer


router = APIRouter(prefix="/prediction")


@router.get("")
async def predict(request: Request, producer: AIOKafkaProducer = Depends(get_kafka_producer)):
    request_id = request.state.request_id
    logger.info(
        f"New predict request received: request_id={request_id}",
        extra={"tags": {"service": "gate", "endpoint": "/predict"}},
    )

    message = {"request_id": request_id, "prompt": "Generate a text based on user input"}
    try:
        await producer.send_and_wait(topic="predict_topic", value=json.dumps(message).encode("utf-8"))
        logger.info(
            f"Message sent to Kafka topic 'predict_topic': {message}",
            extra={"tags": {"service": "gate", "endpoint": "/predict"}},
        )
    except Exception as e:
        prometheus_metrics.ERROR_COUNT.labels(request.url.path, type(e).__name__).inc()
        logger.error(
            f"Failed to send message to Kafka topic 'predict_topic': {e}",
            extra={"tags": {"service": "gate", "endpoint": "/predict"}},
        )
        raise

    return {"message": "Request sent to Predict_service", "request_id": request_id}
