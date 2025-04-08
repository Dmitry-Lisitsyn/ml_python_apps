import json

from aiokafka import AIOKafkaProducer
from fastapi import APIRouter, Request, Depends

from api.v1.shemas import GenerationRequest
from settings import logger
from services.redis import response, request as redis_request
from services.kafka import get_kafka_producer


router = APIRouter(prefix="/generation")


@router.post("")
async def generate(gen_req: GenerationRequest, req: Request, producer: AIOKafkaProducer = Depends(get_kafka_producer)):
    request_id = req.state.request_id
    logger.info(
        f"New generate request received: request_id={request_id}",
        extra={"tags": {"service": "gate", "endpoint": "/generation"}},
    )

    if cache_request := await redis_request.get_request(gen_req.prompt):
        logger.info(
            f"Cache hit for request_id={request_id}",
            extra={"tags": {"service": "gate", "endpoint": "/generation"}}
        )
        return cache_request

    await response.add_response(request_id)
    logger.info(
        f"Sending message to Kafka topic 'llm_prompts': 'request_id': {request_id}, 'prompt': {gen_req.prompt}",
        extra={"tags": {"service": "gate", "endpoint": "/generation"}},
    )

    try:
        await producer.send_and_wait(
            topic="llm_prompts",
            value=json.dumps(
                {
                    "request_id": request_id,
                    "system_context": gen_req.system_context,
                    "prompt": gen_req.prompt
                }
            ).encode("utf-8")
        )
    except Exception as e:
        logger.error(
            f"Failed to send message to Kafka topic 'llm_prompts': {e}",
            extra={"tags": {"service": "gate", "endpoint": "/generation"}},
        )
        raise

    updated_response = await response.waiting_updating_response(request_id, gen_req.prompt)
    if updated_response:
        logger.info(
            f"Generate response completed: {updated_response}",
            extra={"tags": {"service": "gate", "endpoint": "/generation"}},
        )
        return updated_response

    logger.error(
        f"Timeout occurred for request_id={request_id}",
        extra={"tags": {"service": "gate", "endpoint": "/generation"}},
    )
    return {"result": "Request timed out", "status": "error", "request_id": request_id}
