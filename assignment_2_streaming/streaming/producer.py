#!/usr/bin/env python3
import argparse
import heapq
import json
import logging
import random
import re
import signal
import sys
import time
import zlib
from datetime import datetime
from pathlib import Path
from typing import Iterator, List, Optional

BASE_DIR = Path(__file__).resolve().parents[2]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from assignment_2_streaming.streaming import metrics
from assignment_2_streaming.streaming.config import RAW_DATA_DIR, StreamingConfig
from common.logging_utils import setup_logging

logger = logging.getLogger("streaming.producer")

MIN_RATE, MAX_RATE = 10, 50
_MONTH_IN_NAME = re.compile(r"(\d{4}-\d{2})")


def iter_source_trips(data_file: Path, limit: Optional[int] = None, fetch_size: int = 2000) -> Iterator[dict]:
    import duckdb

    match = _MONTH_IN_NAME.search(data_file.name)
    if not match:
        raise ValueError(f"Cannot infer month from file name {data_file.name}")
    period_start = datetime.strptime(match.group(1) + "-01", "%Y-%m-%d")
    period_end = datetime(period_start.year + (period_start.month == 12), period_start.month % 12 + 1, 1)

    path = data_file.as_posix().replace("'", "''")
    sql = f"""
        SELECT
            file_row_number,
            VendorID              AS vendor_id,
            tpep_pickup_datetime  AS pickup_datetime,
            tpep_dropoff_datetime AS dropoff_datetime,
            PULocationID          AS pulocation_id,
            DOLocationID          AS dolocation_id,
            passenger_count,
            trip_distance,
            payment_type,
            fare_amount,
            tip_amount,
            total_amount
        FROM read_parquet('{path}', file_row_number = true)
        WHERE tpep_pickup_datetime >= ? AND tpep_pickup_datetime < ?
        ORDER BY tpep_pickup_datetime, file_row_number
    """
    if limit:
        sql += f" LIMIT {int(limit)}"

    con = duckdb.connect()
    try:
        cursor = con.execute(sql, [period_start, period_end])
        names = [column[0] for column in cursor.description]
        while True:
            rows = cursor.fetchmany(fetch_size)
            if not rows:
                break
            for row in rows:
                yield dict(zip(names, row))
    finally:
        con.close()


def build_event(row: dict, source_name: str, realtime_timestamps: bool = False) -> dict:
    pickup, dropoff = row["pickup_datetime"], row["dropoff_datetime"]
    if realtime_timestamps:
        now = datetime.utcnow().replace(microsecond=0)
        pickup, dropoff = now, now + (dropoff - pickup)

    def number(value):
        return None if value is None or value != value else value

    passenger_count = number(row["passenger_count"])
    return {
        "trip_id": f"{source_name}-{row['file_row_number']}",
        "vendor_id": number(row["vendor_id"]),
        "pickup_datetime": pickup.isoformat(),
        "dropoff_datetime": dropoff.isoformat(),
        "pulocation_id": int(row["pulocation_id"]),
        "dolocation_id": int(row["dolocation_id"]),
        "passenger_count": int(passenger_count) if passenger_count is not None else None,
        "trip_distance": number(row["trip_distance"]),
        "payment_type": number(row["payment_type"]),
        "fare_amount": number(row["fare_amount"]),
        "tip_amount": number(row["tip_amount"]),
        "total_amount": number(row["total_amount"]),
    }


class OutOfOrderInjector:
    def __init__(self, ratio: float, max_delay_events: int, seed: int = 42):
        self.ratio = ratio
        self.max_delay_events = max_delay_events
        self._rng = random.Random(seed)
        self._pending: List[tuple] = []
        self._sequence = 0

    def push(self, event: dict) -> List[dict]:
        self._sequence += 1
        ready = []
        while self._pending and self._pending[0][0] <= self._sequence:
            ready.append(heapq.heappop(self._pending)[2])
        if self.ratio > 0 and self._rng.random() < self.ratio:
            delay = self._rng.randint(1, self.max_delay_events)
            heapq.heappush(self._pending, (self._sequence + delay, self._sequence, event))
            metrics.PRODUCER_OUT_OF_ORDER_INJECTED.inc()
        else:
            ready.append(event)
        return ready

    def drain(self) -> List[dict]:
        remaining = [item[2] for item in sorted(self._pending)]
        self._pending.clear()
        return remaining


class RateLimiter:
    def __init__(self, events_per_second: float):
        self.interval = 1.0 / events_per_second
        self._next_send = time.monotonic()

    def wait(self) -> None:
        now = time.monotonic()
        if self._next_send > now:
            time.sleep(self._next_send - now)
        elif now - self._next_send > 1.0:
            self._next_send = now
        self._next_send += self.interval


class TaxiStreamProducer:
    def __init__(self, cfg: StreamingConfig, dry_run: bool = False):
        self.cfg = cfg
        self.dry_run = dry_run
        self.producer = None if dry_run else self._create_producer()
        self._stop = False

    def _create_producer(self):
        from kafka import KafkaProducer

        options = dict(
            bootstrap_servers=self.cfg.bootstrap_list,
            client_id="taxi-trip-producer",
            acks="all",
            retries=10,
            retry_backoff_ms=200,
            max_in_flight_requests_per_connection=1,
            linger_ms=20,
            batch_size=32 * 1024,
            compression_type="gzip",
            max_block_ms=10_000,
            request_timeout_ms=30_000,
        )
        if "buffer_memory" in KafkaProducer.DEFAULT_CONFIG:
            options["buffer_memory"] = 32 * 1024 * 1024
        if "enable_idempotence" in KafkaProducer.DEFAULT_CONFIG:
            options["enable_idempotence"] = True
        producer = KafkaProducer(**options)
        logger.info("connected to Kafka", extra={"bootstrap_servers": self.cfg.bootstrap_servers,
                                                 "idempotent": options.get("enable_idempotence", False)})
        return producer

    def stop(self, *_):
        self._stop = True

    def _on_success(self, sent_at: float, record_metadata) -> None:
        metrics.PRODUCER_EVENTS_SENT.labels(topic=record_metadata.topic, partition=str(record_metadata.partition)).inc()
        metrics.PRODUCER_BYTES_SENT.labels(topic=record_metadata.topic).inc(max(record_metadata.serialized_value_size, 0))
        metrics.PRODUCER_ACK_LATENCY.observe(time.monotonic() - sent_at)

    def _on_error(self, exc: Exception) -> None:
        metrics.PRODUCER_SEND_ERRORS.labels(topic=self.cfg.topic, error=type(exc).__name__).inc()
        logger.error("delivery failed", extra={"error": repr(exc)})

    def send(self, event: dict) -> None:
        event["produced_at"] = time.time()
        key = str(event["pulocation_id"])
        if self.dry_run:
            partition = zlib.crc32(key.encode()) % self.cfg.num_partitions
            metrics.PRODUCER_EVENTS_SENT.labels(topic=self.cfg.topic, partition=str(partition)).inc()
            return

        from kafka.errors import KafkaTimeoutError

        value = json.dumps(event, separators=(",", ":")).encode("utf-8")
        while not self._stop:
            try:
                future = self.producer.send(self.cfg.topic, key=key.encode("utf-8"), value=value)
                future.add_callback(self._on_success, time.monotonic())
                future.add_errback(self._on_error)
                return
            except KafkaTimeoutError:
                metrics.PRODUCER_SEND_ERRORS.labels(topic=self.cfg.topic, error="BufferFullTimeout").inc()
                logger.warning("producer buffer full, retrying send", extra={"trip_id": event["trip_id"]})

    def run(self, data_file: Path, rate: int, max_events: Optional[int], late_ratio: float,
            max_delay_events: int, realtime_timestamps: bool = False) -> int:
        metrics.PRODUCER_TARGET_RATE.set(rate)
        limiter = RateLimiter(rate)
        injector = OutOfOrderInjector(late_ratio, max_delay_events)
        source_name = data_file.stem
        sent, started = 0, time.monotonic()
        logger.info("stream started", extra={"source": data_file.name, "rate_eps": rate, "max_events": max_events,
                                             "late_ratio": late_ratio, "dry_run": self.dry_run})

        def emit(events: List[dict]) -> None:
            nonlocal sent
            for event in events:
                limiter.wait()
                self.send(event)
                sent += 1
                if sent % 250 == 0:
                    elapsed = time.monotonic() - started
                    logger.info("progress", extra={"sent": sent, "actual_rate_eps": round(sent / elapsed, 1),
                                                   "last_event_time": event["pickup_datetime"]})

        try:
            for row in iter_source_trips(data_file, limit=max_events):
                if self._stop:
                    break
                emit(injector.push(build_event(row, source_name, realtime_timestamps)))
            if not self._stop:
                emit(injector.drain())
        finally:
            if self.producer is not None:
                self.producer.flush(timeout=30)
                self.producer.close(timeout=30)

        elapsed = time.monotonic() - started
        logger.info("stream finished", extra={"sent": sent, "duration_sec": round(elapsed, 1),
                                              "actual_rate_eps": round(sent / max(elapsed, 0.001), 1)})
        return sent


def rate_type(value: str) -> int:
    rate = int(value)
    if not MIN_RATE <= rate <= MAX_RATE:
        raise argparse.ArgumentTypeError(f"rate must be between {MIN_RATE} and {MAX_RATE} events/second")
    return rate


def main() -> None:
    cfg = StreamingConfig()
    parser = argparse.ArgumentParser(description="NYC taxi Kafka stream producer")
    parser.add_argument("--rate", type=rate_type, default=min(max(cfg.events_per_second, MIN_RATE), MAX_RATE),
                        help="Events per second (10-50)")
    parser.add_argument("--max-events", type=int, default=None, help="Stop after N events (default: whole month)")
    parser.add_argument("--month", default="2023-01", help="Source month (YYYY-MM)")
    parser.add_argument("--late-ratio", type=float, default=0.05,
                        help="Fraction of events delayed to simulate out-of-order arrival (0 disables)")
    parser.add_argument("--max-delay-events", type=int, default=300,
                        help="Maximum delay of an out-of-order event, in positions")
    parser.add_argument("--realtime-timestamps", action="store_true",
                        help="Replace historical pickup times with the current UTC time")
    parser.add_argument("--dry-run", action="store_true", help="Do not connect to Kafka; only exercise the source")
    args = parser.parse_args()

    setup_logging("stream_producer")
    data_file = RAW_DATA_DIR / f"yellow_tripdata_{args.month}.parquet"
    if not data_file.exists():
        from assignment_1_batch.scripts.download_data import download_month_data

        data_file = download_month_data(args.month, RAW_DATA_DIR)

    metrics.start_metrics_server(cfg.producer_metrics_port)
    producer = TaxiStreamProducer(cfg, dry_run=args.dry_run)
    signal.signal(signal.SIGINT, producer.stop)
    signal.signal(signal.SIGTERM, producer.stop)
    producer.run(data_file, args.rate, args.max_events, args.late_ratio, args.max_delay_events,
                 args.realtime_timestamps)


if __name__ == "__main__":
    main()
