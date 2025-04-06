import asyncio
import json
import uvicorn
from logger_utils import setup_logger
from fastapi import FastAPI
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from contextlib import asynccontextmanager
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import Counter, Gauge
from ollama import Client

app = FastAPI()

client = Client(
  host='http://localhost:11434',
)

# Метрики Prometheus
kafka_messages_received = Counter(
    "kafka_messages_received_total", "Total number of messages received from Kafka"
)
kafka_messages_processed = Counter(
    "kafka_messages_processed_total", "Total number of successfully processed messages"
)
kafka_messages_failed = Counter(
    "kafka_messages_failed_total", "Total number of failed message processing attempts"
)
kafka_consumer_lag = Gauge(
    "kafka_consumer_lag", "Kafka Consumer Lag (message delay)"
)

generator = None
consumer = None
producer = None

logger = setup_logger()

@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer, consumer, generator

    logger.info("Application is starting", extra={"tags": {"service": "ml-llm_service"}})

    try:
        producer = AIOKafkaProducer(bootstrap_servers="localhost:9092")
        await producer.start()
        logger.info("Kafka producer started successfully", extra={"tags": {"service": "ml-llm_service"}})
    except Exception as e:
        logger.error(f"Failed to start Kafka producer: {e}", extra={"tags": {"service": "ml-llm_service"}})
        raise

    try:
        loop = asyncio.get_event_loop()
        consumer = AIOKafkaConsumer(
            "llm_prompts",
            loop=loop,
            client_id='all',
            bootstrap_servers="localhost:9092",
            enable_auto_commit=False,
        )
        asyncio.create_task(consume_llm_process())
        logger.info("Kafka consumer started successfully", extra={"tags": {"service": "ml-llm_service"}})
    except Exception as e:
        logger.error(f"Failed to start Kafka consumer: {e}", extra={"tags": {"service": "ml-llm_service"}})
        raise

    # try:
    #     if "GIGACHAT_CREDENTIALS" not in os.environ:
    #         api_key = ''
    #         GigaChatGenerator.set_credentials(api_key)
    #     generator = GigaChatGenerator()
    #     logger.info("GigaChatGenerator initialized successfully", extra={"tags": {"service": "ml-llm_service"}})
    # except Exception as e:
    #     logger.error(f"Failed to initialize GigaChatGenerator: {e}", extra={"tags": {"service": "ml-llm_service"}})
    #     raise

    yield

    try:
        await producer.stop()
        logger.info("Kafka producer stopped successfully", extra={"tags": {"service": "ml-llm_service"}})
    except Exception as e:
        logger.error(f"Failed to stop Kafka producer: {e}", extra={"tags": {"service": "ml-llm_service"}})

    try:
        await consumer.stop()
        logger.info("Kafka consumer stopped successfully", extra={"tags": {"service": "ml-llm_service"}})
    except Exception as e:
        logger.error(f"Failed to stop Kafka consumer: {e}", extra={"tags": {"service": "ml-llm_service"}})


app.router.lifespan_context = lifespan

# Инструментируем приложение для сбора метрик
Instrumentator().instrument(app).expose(app, endpoint="/metrics")

@app.on_event("shutdown")
async def on_shutdown():
    global consumer
    if consumer:
        await consumer.stop()

@app.get("/")
async def root():
    logger.info("Health check endpoint accessed", extra={"tags": {"service": "ml-llm_service", "endpoint": "/"}})
    return {"message": "FastAPI is running"}

async def consume_llm_process():
    global consumer
    await consumer.start()
    try:
        async for msg in consumer:
            kafka_messages_received.inc()
            message = json.loads(msg.value.decode("utf-8"))
            logger.info(f"Received message from Kafka topic 'llm_prompts': {message}", extra={"tags": {"service": "ml-llm_service"}})

            request_id = message.get("request_id")
            system_context = message.get("system_context")
            prompt = message.get("prompt")

            if not prompt:
                logger.warning(f"Invalid message format: {message}", extra={"tags": {"service": "ml-llm_service"}})
                continue

            try:

                response = client.chat(model='llama3.2:latest', messages=[
                    {
                        'role': 'system',
                        'content': system_context,
                    },
                    {
                        'role': 'user',
                        'content': prompt,
                    },
                ],
                options={"temperature": 0},
                format='json'
                )
                print(response['message']['content'])
                #response = generator.generate_response(system_context, prompt)

                #response = "Сгенерированный ответ из ML сервиса"
                response = response['message']['content']
                logger.info(f"Generated response for request_id={request_id}: {response}", extra={"tags": {"service": "ml-llm_service"}})

                await process_llm_result(request_id, "success", response)
                kafka_messages_processed.inc()
            except Exception as e:
                logger.error(f"Error generating response for request_id={request_id}: {e}", extra={"tags": {"service": "ml-llm_service"}})
                await process_llm_result(request_id, "error", repr(e))
                kafka_messages_failed.inc()
    except Exception as e:
        logger.error(f"Error while processing Kafka messages: {e}", extra={"tags": {"service": "ml-llm_service"}})
    finally:
        await consumer.stop()

async def process_llm_result(request_id, status, response):
    message = {"request_id": request_id, "status": status, "result": response}
    logger.info(f"Sending result to Kafka topic 'llm_responses': {message}", extra={"tags": {"service": "ml-llm_service"}})
    await producer.send_and_wait(topic="llm_responses", value=json.dumps(message).encode("utf-8"))

if __name__ == "__main__":
    logger.info("Starting ML LLM Service with Uvicorn", extra={"tags": {"service": "ml-llm_service"}})
    uvicorn.run(app, host="0.0.0.0", port=8001)
