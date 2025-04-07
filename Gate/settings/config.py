import logging

from pydantic import BaseModel


class LogConfig(BaseModel):
    """Константы логирования."""
    LOKI_URL: str = "http://localhost:3100/loki/api/v1/push"
    LOKI_TAGS: dict[str, str] = {"application": "ml-gate"}
    LOKI_LOGGER_NAME: str = "ml-gate"
    LOG_FORMAT: str = "%(filename)s[LINE:%(lineno)d]# %(levelname)-8s %(message)s"
    LOG_LEVEL: int = logging.DEBUG


class KafkaConfig(BaseModel):
    KAFKA_BROKER: str = "localhost:9092"
    KAFKA_TOPIC_REQUESTS: str = "llm_prompts"
    KAFKA_TOPIC_RESPONSES: str = "llm_responses"


class RedisConfig(BaseModel):
    HOST: str = "localhost"
    PORT: int = 6380
    ENCODING: str = "utf-8"


log_config = LogConfig()
kafka_config = KafkaConfig()
redis_config = RedisConfig()
