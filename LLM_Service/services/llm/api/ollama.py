from langchain_ollama import ChatOllama

from services.llm.api.base_llm import BaseLLM


class OllamaApi(BaseLLM):
    def __init__(self):
        client = ChatOllama(
            model="llama3.2:latest",
            base_url="http://localhost:11434",
            temperature=0,
            format="json"
        )
        super().__init__(client)
