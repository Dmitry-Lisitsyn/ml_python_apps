import redis.asyncio as async_redis

from settings import redis_config, logger


class RedisManager:

    def __init__(self):
        self.client = self.init_redis()

    @staticmethod
    def init_redis():
        try:
            redis = async_redis.Redis(
                host=redis_config.HOST,
                port=redis_config.PORT,
                encoding=redis_config.ENCODING,
                decode_responses=True
            )
            logger.info(
                        "Redis connection established successfully",
                        extra={"tags": {"service": "gate"}},
            )
            return redis
        except Exception as e:
            logger.exception(
                f"Failed to connect to the Redis: {e}",
                extra={"tags": {"service": "gate"}},
            )
            raise

    async def flush_db(self):
        try:
            await self.client.flushdb()
            logger.info(
                "Cleared cached generate results from Redis",
                extra={"tags": {"service": "gate"}},
            )
        except Exception as e:
            logger.error(
                f"Failed to clear db data: {e}",
                extra={"tags": {"service": "gate"}},
            )


redis_manager = RedisManager()
redis_client = redis_manager.client
