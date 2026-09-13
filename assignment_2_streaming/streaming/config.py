from dataclasses import dataclass, field
from typing import List

from common.db import DatabaseConfig
from common.settings import BASE_DIR, RAW_DATA_DIR, env_float, env_int, env_str

__all__ = ["RAW_DATA_DIR", "STREAMING_SQL_DIR", "StreamingConfig", "streaming_db_config"]

STREAMING_SQL_DIR = BASE_DIR / "assignment_2_streaming" / "sql"


@dataclass
class StreamingConfig:
    bootstrap_servers: str = field(
        default_factory=lambda: env_str("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092,localhost:9094,localhost:9096"))
    topic: str = field(default_factory=lambda: env_str("KAFKA_TOPIC", "taxi-trips-stream"))
    dlq_topic: str = field(default_factory=lambda: env_str("KAFKA_DLQ_TOPIC", "taxi-trips-dlq"))
    consumer_group: str = field(default_factory=lambda: env_str("KAFKA_CONSUMER_GROUP", "taxi-stream-analytics-group"))
    num_partitions: int = field(default_factory=lambda: env_int("KAFKA_TOPIC_PARTITIONS", 6))

    events_per_second: int = field(default_factory=lambda: env_int("STREAM_EVENTS_PER_SECOND", 25))

    window_size_sec: int = field(default_factory=lambda: env_int("STREAM_WINDOW_SIZE_MINUTES", 5) * 60)
    slide_sec: int = field(default_factory=lambda: env_int("STREAM_SLIDE_STEP_MINUTES", 1) * 60)
    allowed_lateness_sec: int = field(default_factory=lambda: env_int("STREAM_WATERMARK_LATENESS_MINUTES", 2) * 60)
    max_future_skew_sec: int = field(default_factory=lambda: env_int("STREAM_MAX_FUTURE_SKEW_MINUTES", 60) * 60)

    poll_max_records: int = field(default_factory=lambda: env_int("STREAM_POLL_MAX_RECORDS", 200))
    poll_min_records: int = field(default_factory=lambda: env_int("STREAM_POLL_MIN_RECORDS", 10))
    backpressure_latency_sec: float = field(default_factory=lambda: env_float("STREAM_BACKPRESSURE_LATENCY_SEC", 1.0))
    backpressure_pause_sec: float = field(default_factory=lambda: env_float("STREAM_BACKPRESSURE_PAUSE_SEC", 2.0))

    producer_metrics_port: int = field(default_factory=lambda: env_int("PRODUCER_METRICS_PORT", 8001))
    consumer_metrics_port: int = field(
        default_factory=lambda: env_int("CONSUMER_METRICS_PORT", env_int("PROMETHEUS_METRICS_PORT", 8000)))

    @property
    def bootstrap_list(self) -> List[str]:
        return [server.strip() for server in self.bootstrap_servers.split(",") if server.strip()]


def streaming_db_config() -> DatabaseConfig:
    return DatabaseConfig.from_env(
        prefix="STREAM_",
        default_db="nyc_taxi_streaming",
        default_port=5433,
        default_duckdb_path="data/nyc_taxi_streaming.duckdb",
    )
