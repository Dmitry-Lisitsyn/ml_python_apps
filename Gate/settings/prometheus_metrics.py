from prometheus_client import Counter, Histogram, Gauge

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
