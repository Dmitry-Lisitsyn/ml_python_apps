from aiokafka import AIOKafkaConsumer
import asyncio

KAFKA_BROKER = "localhost:9092"  # Замените на адрес вашего брокера Kafka
KAFKA_TOPIC_REQUESTS = "llm_prompts"  # Тема для запросов LLM

class AIOWebConsumer:
    def __init__(self, loop, topic):
        self.__loop = loop
        self.__consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BROKER,
            loop=loop,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="llm_group"
        )
        self.__started = False

    async def start(self):
        if not self.__started:
            await self.__consumer.start()
            self.__started = True

    async def stop(self):
        if self.__started:
            await self.__consumer.stop()
            self.__started = False

    async def consume(self):
        """
        Метод для получения сообщений из Kafka.
        """
        try:
            async for msg in self.__consumer:
                yield msg
        except Exception as e:
            print(f"Ошибка в консьюмере: {e}")
        finally:
            await self.stop()


# Функция для создания консьюмера
def get_consumer(loop, topic) -> AIOWebConsumer:
    return AIOWebConsumer(loop, topic)