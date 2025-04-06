import os
from typing import List, Dict, Optional
from langchain_gigachat import GigaChat
from langchain_core.messages import HumanMessage, SystemMessage


class GigaChatGenerator:
    def __init__(self, credentials: Optional[str] = None, verify_ssl_certs: bool = False):

        # Получаем API-ключ из переменной окружения или параметра
        self.credentials = credentials or os.getenv("GIGACHAT_CREDENTIALS")
        if not self.credentials:
            raise ValueError(
                "API credentials are missing. "
                "Set the 'GIGACHAT_CREDENTIALS' environment variable or pass it as a parameter."
            )

        # Инициализация GigaChat
        self.chat = GigaChat(verify_ssl_certs=verify_ssl_certs, scope="GIGACHAT_API_PERS")

    def generate_response(self, system_prompt: str, user_prompt: str) -> str:
        """
        Генерирует ответ от GigaChat на основе системного и пользовательского сообщений.

        :param system_prompt: Системное сообщение (инструкция для модели).
        :param user_prompt: Сообщение пользователя.
        :return: Ответ модели.
        """
        messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=user_prompt),
        ]
        response = self.chat.invoke(messages)
        return response.content

    @staticmethod
    def set_credentials(credentials: str):
        os.environ["GIGACHAT_CREDENTIALS"] = credentials


# # Пример использования модуля
# if __name__ == "__main__":
#     # Установка API-ключа (если он еще не установлен)
#     if "GIGACHAT_CREDENTIALS" not in os.environ:
#         api_key = 'ZmY3NTQ4N2QtMTNiMi00ZDFiLTgzOTUtNWMyYjVkMWEyYjM1OjY2OTdkMWEwLTAwY2QtNDEyNi04ODVjLTNiYWQ2ZmZkOGU0Mw=='
#         GigaChatGenerator.set_credentials(api_key)
#
#     # Создание экземпляра генератора
#     generator = GigaChatGenerator()
#
#     # Генерация ответа
#     system_prompt = "You are a helpful AI that shares everything you know. Talk in English."
#     user_prompt = "What is the capital of Russia?"
#     response = generator.generate_response(system_prompt, user_prompt)
#
#     # Вывод результата
#     print(response)