import logging

from pydantic import BaseModel


SERVICE_NAME = "ml-llm_service"


class LogConfig(BaseModel):
    """Константы логирования."""
    LOKI_URL: str = "http://localhost:3100/loki/api/v1/push"
    LOKI_LOGGER_NAME: str = SERVICE_NAME
    LOKI_TAGS: dict[str, str] = {"application": LOKI_LOGGER_NAME}
    LOG_FORMAT: str = "%(filename)s[LINE:%(lineno)d]# %(levelname)-8s %(message)s"
    LOG_LEVEL: int = logging.DEBUG


class KafkaConfig(BaseModel):
    KAFKA_BROKER: str = "localhost:9092"
    KAFKA_TOPIC_REQUESTS: str = "llm_prompts"
    KAFKA_TOPIC_RESPONSES: str = "llm_responses"
    CLIENT_ID: str = 'all'


class GigaConfig(BaseModel):
    GIGA_SCOPE: str = 'GIGACHAT_API_CORP'
    GIGA_MODEL: str = 'GigaChat-Max'
    TEMPERATURE: float = 1e-6
    MAX_TOKENS: int = 2_000


log_config = LogConfig()
kafka_config = KafkaConfig()
giga_config = GigaConfig()
