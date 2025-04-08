import logging
from logging import Logger

import logging_loki

from settings.config import log_config


def get_loki_handler() -> logging.Handler:
    return logging_loki.LokiHandler(
        url=log_config.LOKI_URL,
        tags=log_config.LOKI_TAGS,
        version="1",
        # auth=("username", "password"),
    )


def setup_logger(name: str = log_config.LOKI_LOGGER_NAME) -> Logger:
    configured_logger = logging.getLogger(name)
    if not configured_logger .handlers:
        handler = get_loki_handler()
        handler.setFormatter(logging.Formatter(fmt=log_config.LOG_FORMAT))
        configured_logger .setLevel(log_config.LOG_LEVEL)
        configured_logger .addHandler(handler)
    return configured_logger


logger = setup_logger()
