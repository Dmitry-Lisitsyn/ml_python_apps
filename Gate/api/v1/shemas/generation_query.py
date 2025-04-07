from pydantic import BaseModel, Field


class GenerationRequest(BaseModel):
    system_context: str = Field(description="Системный промпт")
    prompt: str = Field(description="Пользовательский промпт")
