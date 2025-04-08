import asyncio
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator

from services import kafka
from settings import logger, SERVICE_NAME


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Application is starting", extra={"tags": {"service": SERVICE_NAME}})
    loop = asyncio.get_event_loop()
    await kafka.init_producer()
    await kafka.init_consumer(loop)
    yield
    await kafka.close_producer()
    await kafka.close_consumer()
    logger.info("Application stopped successfully", extra={"tags": {"service": SERVICE_NAME}})


app = FastAPI(lifespan=lifespan)
Instrumentator().instrument(app).expose(app, endpoint="/metrics")  # Инструментируем приложение для сбора метрик


@app.get("/")
async def root():
    logger.info("Health check endpoint accessed", extra={"tags": {"service": "ml-llm_service", "endpoint": "/"}})
    return {"message": "FastAPI is running"}


if __name__ == "__main__":
    logger.info("Starting ML LLM Service with Uvicorn", extra={"tags": {"service": "ml-llm_service"}})
    uvicorn.run(app, host="0.0.0.0", port=8001)
