from enum import Enum

from pydantic import Field

from dotenv import load_dotenv
from pydantic_settings import BaseSettings

__all__ = ['settings', 'LLMType']

load_dotenv()


class LLMType(str, Enum):
    ollama = 'ollama'
    giga = 'giga'


def str_to_bool(value: str | bool) -> bool:
    """
    Преобразование из str в bool.

    :param value: Значение.
    :return:      Bool значение.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        value = value.lower().strip().replace('\'', '').replace('\"', '')
        if value == 'true':
            return True
        elif value == 'false':
            return False
        raise ValueError('Неверное значение, может быть только True или False')
    raise ValueError('Неподходящий формат данных')


class Settings(BaseSettings):

    LLM_MODEL: str = Field(validation_alias='LLM_MODEL')
    GIGA_CREDENTIALS: str = Field(validation_alias='GIGA_CREDENTIALS')
    # OPENAI_API_KEY: str = Field(validation_alias='OPENAI_API_KEY', default='')

    @property
    def debug_mode(self):
        return str_to_bool(self.LOCAL_DEPLOY)


settings = Settings()
