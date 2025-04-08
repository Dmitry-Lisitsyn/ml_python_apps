from prometheus_client import Counter, Gauge

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
