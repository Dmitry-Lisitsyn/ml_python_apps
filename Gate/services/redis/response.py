import asyncio
import time
import json

from services.redis import redis_client, request

RESPONSE_TIMEOUT = 30


async def get_response(request_id: int) -> dict | None:
    response = await redis_client.hget(str(request_id), key='response')
    return json.loads(response) if response else None


async def add_response(request_id: int) -> None:
    response = {
        "result": None,
        "status": None,
        "created_at": time.time(),
    }
    response_serialized = json.dumps(response)
    await redis_client.hset(name=str(request_id), key='response', value=response_serialized)
    await redis_client.expire(name=str(request_id), time=30)


async def update_response(request_id: int, result: str, status: str) -> bool:
    response = await get_response(request_id)
    if not response:
        return False
    response['result'], response['status'] = result, status
    await redis_client.hset(name=str(request_id), key='response', value=json.dumps(response))
    return True


async def pop_response(request_id: int) -> dict | None:
    response = await get_response(request_id)
    if response:
        await redis_client.delete(str(request_id))
    return response


async def waiting_updating_response(request_id: int, prompt: str) -> dict | None:
    start_time = time.time()
    while time.time() - start_time < RESPONSE_TIMEOUT:
        response = await get_response(request_id)
        if isinstance(response, dict) and response.get('status'):
            await pop_response(request_id)
            await request.add_request(prompt, response)
            return response
        await asyncio.sleep(0.5)  # пауза между проверками
