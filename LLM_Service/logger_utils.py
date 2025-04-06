import logging_loki
import logging


def setup_logger():
    handler = get_logger_handler()
    logger = logging.getLogger("ml-llm_service")
    formatter = logging.Formatter(fmt=u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    return logger
def get_logger_handler():
    return logging_loki.LokiHandler(
        url="http://localhost:3100/loki/api/v1/push",
        tags={"application": "ml-llm_service"},
        # auth=("username", "password"),
        version="1",
    )
