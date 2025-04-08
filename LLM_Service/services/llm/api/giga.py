from langchain_gigachat import GigaChat

from settings import settings, giga_config
from services.llm.api.stream import StreamHandler
from services.llm.api.base_llm import BaseLLM


class GigaApi(BaseLLM):

    def __init__(self):
        client = GigaChat(
            credentials=settings.GIGA_CREDENTIALS,
            scope=giga_config.GIGA_SCOPE,
            model=giga_config.GIGA_MODEL,
            temperature=giga_config.TEMPERATURE,
            max_tokens=giga_config.MAX_TOKENS,
            verify_ssl_certs=False,
            profanity_check=False,
            streaming=True,
            callbacks=[StreamHandler()],
            top_p=0,
            repetition_penalty=1,
        )
        super().__init__(client)
