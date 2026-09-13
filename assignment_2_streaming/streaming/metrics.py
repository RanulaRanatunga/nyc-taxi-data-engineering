import logging

from prometheus_client import Counter, Gauge, Histogram, start_http_server

logger = logging.getLogger("streaming.metrics")

LATENCY_BUCKETS = (0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)

PRODUCER_EVENTS_SENT = Counter(
    "kafka_producer_events_sent_total", "Events acknowledged by Kafka", ["topic", "partition"])
PRODUCER_SEND_ERRORS = Counter(
    "kafka_producer_send_errors_total", "Events Kafka failed to acknowledge", ["topic", "error"])
PRODUCER_BYTES_SENT = Counter(
    "kafka_producer_bytes_sent_total", "Serialized payload bytes acknowledged by Kafka", ["topic"])
PRODUCER_ACK_LATENCY = Histogram(
    "kafka_producer_ack_latency_seconds", "Time from send() to broker acknowledgement", buckets=LATENCY_BUCKETS)
PRODUCER_TARGET_RATE = Gauge("kafka_producer_target_rate_eps", "Configured producer rate (events/second)")
PRODUCER_OUT_OF_ORDER_INJECTED = Counter(
    "kafka_producer_out_of_order_injected_total", "Events deliberately delayed to simulate out-of-order arrival")

CONSUMER_EVENTS_CONSUMED = Counter(
    "kafka_consumer_events_consumed_total", "Messages polled from Kafka", ["topic", "consumer_group"])
CONSUMER_LAG_RECORDS = Gauge(
    "kafka_consumer_lag_records", "Log end offset minus consumer position", ["topic", "partition"])
CONSUMER_BATCH_SIZE = Histogram(
    "streaming_batch_size_events", "Messages per processed poll batch", buckets=(1, 5, 10, 25, 50, 100, 200, 500))
EVENTS_PROCESSED = Counter(
    "streaming_events_processed_total",
    "Events by outcome: accepted | out_of_order | late | future | duplicate | malformed", ["status"])
DLQ_EVENTS = Counter("streaming_dlq_events_total", "Malformed events published to the dead-letter topic")
RAW_EVENTS_PERSISTED = Counter("streaming_raw_events_persisted_total", "Raw events written to streaming_taxi_trips")
WINDOW_UPSERTS = Counter(
    "streaming_window_aggregations_upserted_total", "Sliding-window rows upserted", ["final"])
BATCH_PROCESSING_SECONDS = Histogram(
    "streaming_batch_processing_seconds", "Aggregation + persistence time per batch", buckets=LATENCY_BUCKETS)
DB_WRITE_SECONDS = Histogram(
    "streaming_database_write_seconds", "Transactional write time per batch", buckets=LATENCY_BUCKETS)
DB_WRITE_ERRORS = Counter("streaming_database_write_errors_total", "Failed batch writes (batch is retried)")
END_TO_END_LATENCY = Histogram(
    "streaming_end_to_end_latency_seconds", "Producer send time to database commit (wall clock)",
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 120))
WATERMARK_TIMESTAMP = Gauge(
    "streaming_watermark_timestamp_seconds", "Current event-time watermark (unix seconds)")
OPEN_WINDOWS = Gauge("streaming_open_windows", "Sliding window x location aggregates held in memory")
BACKPRESSURE_ACTIVE = Gauge("streaming_backpressure_active", "1 while consumption is paused for backpressure")
BACKPRESSURE_EVENTS = Counter("streaming_backpressure_events_total", "Times consumption was paused")
POLL_MAX_RECORDS = Gauge("streaming_poll_max_records", "Current adaptive max_records per poll")

_started_ports = set()


def start_metrics_server(port: int) -> bool:
    if port in _started_ports:
        return True
    try:
        start_http_server(port)
    except OSError as exc:
        logger.error("metrics port unavailable", extra={"port": port, "error": str(exc)})
        return False
    _started_ports.add(port)
    logger.info("metrics endpoint started", extra={"url": f"http://localhost:{port}/metrics"})
    return True
