import hashlib
import json

from services.redis import redis_client

REQUEST_TIMEOUT = 1800


def get_cache(prompt: str):
    return f"generate:{hashlib.sha256(prompt.encode()).hexdigest()}"


async def add_request(prompt: str, value: dict):
    cache_key = get_cache(prompt)
    await redis_client.setex(cache_key, REQUEST_TIMEOUT, json.dumps(value))


async def get_request(prompt: str) -> dict | None:
    cache_key = get_cache(prompt)
    request = await redis_client.get(cache_key)
    return json.loads(request) if request else None
