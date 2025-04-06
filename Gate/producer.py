from aiokafka import AIOKafkaProducer
import asyncio

event_loop = asyncio.get_event_loop()

KAFKA_BROKER = "localhost:9092"  # Замените на адрес вашего брокера Kafka
KAFKA_TOPIC_REQUESTS = "llm_prompts"
class AIOWebProducer:
    def __init__(self, loop):
        self.__loop = loop
        self.__producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            loop=loop
        )
        self.__produce_topic = KAFKA_TOPIC_REQUESTS
        self.__started = False

    async def start(self):
        if not self.__started:
            await self.__producer.start()
            self.__started = True

    async def stop(self):
        if self.__started:
            await self.__producer.stop()
            self.__started = False

    async def send(self, value: bytes):
        if not self.__started:
            await self.start()
        await self.__producer.send(
            topic=self.__produce_topic,
            value=value
        )

def get_producer(loop) -> AIOWebProducer:
    return AIOWebProducer(loop=loop)
