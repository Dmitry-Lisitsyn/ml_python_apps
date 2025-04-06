import asyncio
import hashlib
import json
import time
import uuid
import psutil
import uvicorn
from fastapi import FastAPI, Request, Response, HTTPException
from db_utils import DBUtils
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from contextlib import asynccontextmanager
from logger_utils import setup_logger
from prometheus_client import Counter, Histogram, generate_latest, Gauge
import redis.asyncio as aioredis

app = FastAPI()

response_store = {}

# Настройка логгера
logger = setup_logger()
redis = None
# 🔹 Метрики Prometheus
REQUEST_COUNT = Counter(
    "http_requests_total", "Total HTTP requests", ["method", "endpoint"]
)
REQUEST_LATENCY = Histogram(
    "http_request_duration_seconds", "HTTP request latency", ["method", "endpoint"]
)
ERROR_COUNT = Counter(
    "http_request_errors_total", "Total HTTP Request Errors", ["endpoint", "error_type"]
)
KAFKA_MESSAGES_CONSUMED = Counter(
    "kafka_messages_consumed_total", "Total messages consumed from Kafka", ["topic"]
)
CPU_USAGE = Gauge("cpu_usage_percent", "CPU usage in percent")
MEMORY_USAGE = Gauge("memory_usage_percent", "Memory usage in percent")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_utils, producer, consumer, redis

    # Логирование запуска приложения
    logger.info(
        "Application is starting",
        extra={"tags": {"service": "gate"}},
    )

    try:
        # Инициализация Kafka producer
        producer = AIOKafkaProducer(bootstrap_servers="localhost:9092")
        await producer.start()
        logger.info(
            "Kafka producer started successfully",
            extra={"tags": {"service": "gate"}},
        )
    except Exception as e:
        logger.error(
            f"Failed to start Kafka producer: {e}",
            extra={"tags": {"service": "gate"}},
        )
        raise

    try:
        # Инициализация Kafka consumer
        loop = asyncio.get_event_loop()
        consumer = AIOKafkaConsumer(
            "llm_responses",
            loop=loop,
            bootstrap_servers="localhost:9092",
            enable_auto_commit=False,
        )
        asyncio.create_task(consume_llm_responses())
        logger.info(
            "Kafka consumer started successfully",
            extra={"tags": {"service": "gate"}},
        )
    except Exception as e:
        logger.error(
            f"Failed to start Kafka consumer: {e}",
            extra={"tags": {"service": "gate"}},
        )
        raise

    try:
        # Инициализация базы данных
        db_utils = DBUtils(database_url="postgresql://postgres:postgres@localhost:5433/ml_database")
        await db_utils.connect()
        logger.info(
            "Database connection established successfully",
            extra={"tags": {"service": "gate"}},
        )
    except Exception as e:
        logger.error(
            f"Failed to connect to the database: {e}",
            extra={"tags": {"service": "gate"}},
        )
        raise

    try:
        # Инициализация Redis
        redis = await get_redis()
        logger.info(
            "Redis connection established successfully",
            extra={"tags": {"service": "gate"}},
        )
    except Exception as e:
        logger.error(
            f"Failed to connect to the Redis: {e}",
            extra={"tags": {"service": "gate"}},
        )
        raise

    # Запуск фоновой задачи для очистки хранилища
    asyncio.create_task(cleanup_response_store())

    yield

    # Очистка ресурсов
    try:
        await producer.stop()
        logger.info(
            "Kafka producer stopped successfully",
            extra={"tags": {"service": "gate"}},
        )
    except Exception as e:
        logger.error(
            f"Failed to stop Kafka producer: {e}",
            extra={"tags": {"service": "gate"}},
        )

    try:
        await consumer.stop()
        logger.info(
            "Kafka consumer stopped successfully",
            extra={"tags": {"service": "gate"}},
        )
    except Exception as e:
        logger.error(
            f"Failed to stop Kafka consumer: {e}",
            extra={"tags": {"service": "gate"}},
        )

    try:
        await db_utils.close()
        logger.info(
            "Database connection closed successfully",
            extra={"tags": {"service": "gate"}},
        )
    except Exception as e:
        logger.error(
            f"Failed to close database connection: {e}",
            extra={"tags": {"service": "gate"}},
        )

    try:
        await redis.flushdb()
        logger.info(
            "Cleared cached generate results from Redis",
            extra={"tags": {"service": "gate"}},
        )
    except Exception as e:
        logger.error(
            f"Failed to clear redis data: {e}",
            extra={"tags": {"service": "gate"}},
        )

    logger.info(
        "Application stopped successfully",
        extra={"tags": {"service": "gate"}},
    )


app.router.lifespan_context = lifespan


async def get_redis():
    global redis
    if redis is None:
        redis = await aioredis.from_url("redis://localhost:6380", encoding="utf-8", decode_responses=True)
    return redis


@app.middleware("http")
async def log_requests(request: Request, call_next):
    if request.url.path == "/metrics":
        return await call_next(request)

    REQUEST_COUNT.labels(request.method, request.url.path).inc()

    api_key = request.headers.get("X-API-Key")
    if not api_key:
        logger.warning("Missing API key in request", extra={"tags": {"service": "gate", "endpoint": request.url.path}})
        ERROR_COUNT.labels(request.url.path, type(HTTPException).__name__).inc()
        raise HTTPException(status_code=401, detail="API key is missing")

    #Redis check
    client_id = await redis.get(f"api_key:{api_key}")

    if not client_id:
        client_id = await db_utils.fetchval(
            "SELECT client_id FROM api_keys WHERE api_key = $1", api_key
        )

        if not client_id:
            logger.warning(f"Invalid API key: {api_key}",
                           extra={"tags": {"service": "gate", "endpoint": request.url.path}})
            ERROR_COUNT.labels(request.url.path, type(HTTPException).__name__).inc()
            raise HTTPException(status_code=403, detail="Invalid API key")

        # save to redis
        await redis.setex(f"api_key:{api_key}", 600, str(client_id))

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
    REQUEST_LATENCY.labels(request.method, request.url.path).observe(duration)

    response_body = b""
    async for chunk in response.body_iterator:
        response_body += chunk

    try:
        await db_utils.execute(
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
        ERROR_COUNT.labels(request.url.path, type(e).__name__).inc()
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


# Роуты
@app.get("/predict")
async def predict(request: Request):
    request_id = request.state.request_id
    logger.info(
        f"New predict request received: request_id={request_id}",
        extra={"tags": {"service": "gate", "endpoint": "/predict"}},
    )

    message = {"request_id": request_id, "prompt": "Generate a text based on user input"}
    try:
        await producer.send_and_wait(topic="predict_topic", value=json.dumps(message).encode("utf-8"))
        logger.info(
            f"Message sent to Kafka topic 'predict_topic': {message}",
            extra={"tags": {"service": "gate", "endpoint": "/predict"}},
        )
    except Exception as e:
        ERROR_COUNT.labels(request.url.path, type(e).__name__).inc()
        logger.error(
            f"Failed to send message to Kafka topic 'predict_topic': {e}",
            extra={"tags": {"service": "gate", "endpoint": "/predict"}},
        )
        raise

    return {"message": "Request sent to Predict_service", "request_id": request_id}


@app.post("/generate")
async def generate(request: Request):
    request_id = request.state.request_id
    logger.info(
        f"New generate request received: request_id={request_id}",
        extra={"tags": {"service": "gate", "endpoint": "/generate"}},
    )

    body = await request.json()
    system_context = body.get("system_context", "")
    prompt = body.get("prompt", "")

    cache_key = f"generate:{hashlib.sha256(prompt.encode()).hexdigest()}"

    cached_result = await redis.get(cache_key)
    if cached_result:
        logger.info(f"Cache hit for request_id={request_id}",
        extra={"tags": {"service": "gate", "endpoint": "/generate"}})
        return json.loads(cached_result)

    event = asyncio.Event()
    response_store[request_id] = {
        "event": event,
        "result": None,
        "status": None,
        "created_at": time.time(),
    }

    logger.info(
        f"Sending message to Kafka topic 'llm_prompts': 'request_id': {request_id}, 'prompt': {prompt}",
        extra={"tags": {"service": "gate", "endpoint": "/generate"}},
    )

    try:
        await producer.send_and_wait(topic="llm_prompts", value=json.dumps(
            {"request_id": request_id, "system_context": system_context, "prompt": prompt}).encode("utf-8"))
    except Exception as e:
        logger.error(
            f"Failed to send message to Kafka topic 'llm_prompts': {e}",
            extra={"tags": {"service": "gate", "endpoint": "/generate"}},
        )
        raise

    try:
        await asyncio.wait_for(event.wait(), timeout=30)

        result_data = response_store.pop(request_id)

        result = {
            "result": result_data["result"],
            "status": result_data["status"],
            "request_id": request_id,
        }

        cache_key = f"generate:{hashlib.sha256(prompt.encode()).hexdigest()}"
        await redis.setex(cache_key, 1800, json.dumps(result))

        logger.info(
            f"Generate response completed: {result}",
            extra={"tags": {"service": "gate", "endpoint": "/generate"}},
        )

        return result
    except asyncio.TimeoutError:
        logger.error(
            f"Timeout occurred for request_id={request_id}",
            extra={"tags": {"service": "gate", "endpoint": "/generate"}},
        )
        return {"result": "Request timed out", "status": "error", "request_id": request_id}


async def consume_llm_responses():
    global consumer
    await consumer.start()
    try:
        async for msg in consumer:
            KAFKA_MESSAGES_CONSUMED.labels("llm_responses").inc()
            message = json.loads(msg.value.decode("utf-8"))
            logger.info(
                f"Received message from Kafka topic 'llm_responses': {message}",
                extra={"tags": {"service": "gate"}},
            )

            request_id = message.get("request_id")
            status = message.get("status")
            result = message.get("result")

            if request_id in response_store:
                # Если запрос еще актуален, обновляем результат и устанавливаем событие
                response_store[request_id]["result"] = result
                response_store[request_id]["status"] = status
                response_store[request_id]["event"].set()
                logger.info(
                    f"Updated response_store for request_id={request_id}: result={result}, status={status}",
                    extra={"tags": {"service": "gate"}},
                )
            else:
                # Если запрос уже завершен (таймаут или удаление), логируем сообщение
                logger.warning(
                    f"Received delayed response for request_id={request_id}. Message ignored. Content: {message}",
                    extra={"tags": {"service": "gate"}},
                )
    except Exception as e:
        logger.error(
            f"Error while processing Kafka messages: {e}",
            extra={"tags": {"service": "gate"}},
        )
    finally:
        await consumer.stop()


async def cleanup_response_store():
    """
    Фоновая задача для очистки устаревших записей из response_store.
    """
    while True:
        await asyncio.sleep(10)  # Проверяем каждые 10 секунд
        current_time = time.time()
        expired_ids = [
            request_id
            for request_id, data in response_store.items()
            if current_time - data["created_at"] > 30
        ]
        for request_id in expired_ids:
            logger.info(
                f"Cleaning up expired request_id={request_id} from response_store",
                extra={"tags": {"service": "gate"}},
            )
            response_store.pop(request_id)


@app.get("/metrics")
async def metrics():
    CPU_USAGE.set(psutil.cpu_percent())
    MEMORY_USAGE.set(psutil.virtual_memory().percent)  # исправили на атрибут, не вызываем как функцию
    return Response(generate_latest(), media_type="text/plain")


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
