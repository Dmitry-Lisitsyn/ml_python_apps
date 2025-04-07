import asyncio
import time
import uuid
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Request, Response, HTTPException

from api.v1 import router_v1, metrics_router
from services import kafka, redis_manager, redis_client, db_client
from settings import logger, prometheus_metrics


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Application is starting", extra={"tags": {"service": "gate"}},)

    loop = asyncio.get_event_loop()
    await kafka.init_producer()  # Инициализация Kafka producer
    await kafka.init_consumer(loop)  # Инициализация Kafka consumer
    await db_client.connect()  # Инициализация базы данных

    yield

    # Очистка ресурсов
    await kafka.close_producer()
    await kafka.close_consumer()
    await db_client.close()
    await redis_manager.flush_db()

    logger.info("Application stopped successfully", extra={"tags": {"service": "gate"}})


app = FastAPI(lifespan=lifespan)
app.include_router(router_v1)
app.include_router(metrics_router)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    if request.url.path == "/metrics":
        return await call_next(request)

    prometheus_metrics.REQUEST_COUNT.labels(request.method, request.url.path).inc()

    api_key = request.headers.get("X-API-Key")
    if not api_key:
        logger.warning("Missing API key in request", extra={"tags": {"service": "gate", "endpoint": request.url.path}})
        prometheus_metrics.ERROR_COUNT.labels(request.url.path, type(HTTPException).__name__).inc()
        raise HTTPException(status_code=401, detail="API key is missing")

    # Redis check
    client_id = await redis_client.get(f"api_key:{api_key}")

    if not client_id:
        client_id = await db_client.fetchval(
            "SELECT client_id FROM api_keys WHERE api_key = $1", api_key
        )

        if not client_id:
            logger.warning(f"Invalid API key: {api_key}",
                           extra={"tags": {"service": "gate", "endpoint": request.url.path}})
            prometheus_metrics.ERROR_COUNT.labels(request.url.path, type(HTTPException).__name__).inc()
            raise HTTPException(status_code=403, detail="Invalid API key")

        # save to db
        await redis_client.setex(f"api_key:{api_key}", 600, str(client_id))

    request_id = str(uuid.uuid4())
    request.state.request_id = request_id
    start_time = time.time()

    request_body = await request.body()
    request._body = request_body

    logger.info(
        f"Received request: method={request.method}, path={request.url.path}, client_id={client_id}, request_id={request_id}",
        extra={"tags": {"service": "gate", "endpoint": request.url.path}},
    )
    response = await call_next(request)

    duration = time.time() - start_time
    prometheus_metrics.REQUEST_LATENCY.labels(request.method, request.url.path).observe(duration)

    response_body = b""
    async for chunk in response.body_iterator:
        response_body += chunk

    try:
        await db_client.execute(
            """
            INSERT INTO requests_log (
                id, client_id, endpoint, request_payload, response_payload,
                response_time_ms, model_version, status_code
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            """,
            request_id,
            client_id,
            request.url.path,
            request_body.decode("utf-8") if request.method == "POST" else None,
            response_body.decode("utf-8"),
            int((time.time() - start_time) * 1000),
            None,
            response.status_code,
        )
    except Exception as e:
        prometheus_metrics.ERROR_COUNT.labels(request.url.path, type(e).__name__).inc()
        logger.error(
            f"Ошибка логирования запроса в базу данных: {e}",
            extra={"tags": {"service": "gate", "endpoint": request.url.path}},
        )

    logger.info(
        f"Completed request: method={request.method}, path={request.url.path}, status_code={response.status_code}, request_id={request_id}",
        extra={"tags": {"service": "gate", "endpoint": request.url.path}},
    )

    return Response(
        content=response_body,
        status_code=response.status_code,
        headers=dict(response.headers),
    )


@app.get("/")
async def root():
    logger.info(
        "Health check endpoint accessed",
        extra={"tags": {"service": "gate", "endpoint": "/"}},
    )
    return {"message": "ML API Gateway is running"}


if __name__ == "__main__":
    logger.info(
        "Starting ML API Gateway with Uvicorn",
        extra={"tags": {"service": "gate"}},
    )
    uvicorn.run(app, host="0.0.0.0", port=8000)
