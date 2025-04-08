"""Класс для поддержки потоковой передачи токенов от GigaChat."""
from langchain_core.callbacks.base import BaseCallbackHandler


class StreamHandler(BaseCallbackHandler):
    def __init__(self, initial_text=''):
        pass
