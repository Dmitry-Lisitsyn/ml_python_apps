import asyncio
import httpx

from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage
from langchain_core.language_models.chat_models import BaseChatModel
from gigachat.exceptions import ResponseError

from settings import logger

MAX_RETRIES = 3
BACKOFF_FACTOR = 1


class BaseLLM:

    def __init__(self, client: BaseChatModel):
        self.client = client

    def __str__(self):
        return self.__class__.__name__

    async def ainvoke(self, messages: list[BaseMessage]) -> str | None:
        for attempt in range(MAX_RETRIES):
            try:
                # logger.info(f'Переданные сообщения {self}: {messages}')
                response = await self.client.ainvoke(messages)
                return response.content

            except (ResponseError, httpx.ReadTimeout) as e:
                if isinstance(e, ResponseError):
                    error_args = e.args
                    if len(error_args) < 2 or error_args[1] != 429:
                        print(f'Неизвестная ошибка {type(e)} по запросу {self}: {messages}: {e}')
                        break

                wait_time = BACKOFF_FACTOR * (2 ** attempt)
                print(f'TooManyRequests, повтор через {wait_time} сек. Попытка {attempt + 1}/{MAX_RETRIES}')
                await asyncio.sleep(wait_time)

            except Exception as e:
                print(f'Неизвестная ошибка {type(e)} по запросу {self}: {messages}: {e}')
                break

        return None

    async def get_llm_response(self, system: str | None, user: str) -> str:
        if system:
            messages = [SystemMessage(system), HumanMessage(user)]
        else:
            messages = [HumanMessage(user)]
        response = await self.ainvoke(messages)
        return response

