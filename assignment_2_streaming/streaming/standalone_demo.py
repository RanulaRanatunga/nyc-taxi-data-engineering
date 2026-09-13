#!/usr/bin/env python3
import argparse
import json
import logging
import queue
import sys
import threading
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from assignment_2_streaming.streaming import metrics
from assignment_2_streaming.streaming.config import RAW_DATA_DIR, StreamingConfig, streaming_db_config
from assignment_2_streaming.streaming.events import InvalidEventError, TripEvent
from assignment_2_streaming.streaming.processor import StreamProcessor
from assignment_2_streaming.streaming.producer import (
    OutOfOrderInjector,
    RateLimiter,
    build_event,
    iter_source_trips,
)
from assignment_2_streaming.streaming.storage import StreamingStore
from assignment_2_streaming.streaming.windowing import SlidingWindowAggregator
from common.logging_utils import setup_logging

logger = logging.getLogger("streaming.demo")

_END_OF_STREAM = object()
TOPIC, GROUP = "taxi-trips-stream(in-memory)", "demo"


def run_demo(args) -> None:
    cfg = StreamingConfig()
    metrics.start_metrics_server(cfg.consumer_metrics_port)

    db_cfg = streaming_db_config()
    if args.duckdb:
        db_cfg.engine_type = "duckdb"
    store = StreamingStore.connect(db_cfg, write_delay_sec=args.slow_db_ms / 1000.0)
    store.initialize_schema()
    if args.fresh:
        store.truncate()
    processor = StreamProcessor(store, SlidingWindowAggregator.from_config(cfg))
    processor.rehydrate()

    data_file = RAW_DATA_DIR / f"yellow_tripdata_{args.month}.parquet"
    if not data_file.exists():
        from assignment_1_batch.scripts.download_data import download_month_data

        data_file = download_month_data(args.month, RAW_DATA_DIR, allow_synthetic_fallback=True)

    channel: "queue.Queue" = queue.Queue(maxsize=args.buffer_size)
    produced = {"count": 0, "blocked_sec": 0.0}

    def producer_worker() -> None:
        limiter = RateLimiter(args.rate)
        injector = OutOfOrderInjector(args.late_ratio, args.max_delay_events)

        def emit(events):
            for event in events:
                limiter.wait()
                event["produced_at"] = time.time()
                started = time.monotonic()
                channel.put(json.dumps(event).encode())
                produced["blocked_sec"] += time.monotonic() - started
                produced["count"] += 1
                metrics.PRODUCER_EVENTS_SENT.labels(topic=TOPIC, partition="0").inc()

        for row in iter_source_trips(data_file, limit=args.events):
            emit(injector.push(build_event(row, data_file.stem)))
        emit(injector.drain())
        channel.put(_END_OF_STREAM)

    thread = threading.Thread(target=producer_worker, name="producer", daemon=True)
    started = time.monotonic()
    thread.start()

    totals = {"consumed": 0, "malformed": 0, "duplicates": 0}
    outcomes = {}
    finished = False
    while not finished:
        batch = []
        deadline = time.monotonic() + 0.5
        while len(batch) < 50 and time.monotonic() < deadline:
            try:
                item = channel.get(timeout=max(0.0, deadline - time.monotonic()))
            except queue.Empty:
                break
            if item is _END_OF_STREAM:
                finished = True
                break
            batch.append(item)
        if not batch:
            continue

        metrics.CONSUMER_EVENTS_CONSUMED.labels(topic=TOPIC, consumer_group=GROUP).inc(len(batch))
        metrics.CONSUMER_LAG_RECORDS.labels(topic=TOPIC, partition="0").set(channel.qsize())
        events = []
        for raw in batch:
            try:
                events.append(TripEvent.from_json(raw))
            except InvalidEventError:
                totals["malformed"] += 1
                metrics.EVENTS_PROCESSED.labels(status="malformed").inc()
        result = processor.process(events)
        totals["consumed"] += len(batch)
        totals["duplicates"] += result.duplicates
        for status, count in result.outcomes.items():
            outcomes[status] = outcomes.get(status, 0) + count
        metrics.BACKPRESSURE_ACTIVE.set(1 if channel.full() else 0)

    processor.shutdown_flush()
    elapsed = time.monotonic() - started
    thread.join()
    print_summary(store, produced, totals, outcomes, elapsed)
    store.close()


def print_summary(store: StreamingStore, produced: dict, totals: dict, outcomes: dict, elapsed: float) -> None:
    status_counts = store._query("SELECT event_status, COUNT(*) FROM streaming_taxi_trips GROUP BY event_status")
    window_counts = store._query(
        "SELECT is_final, COUNT(*), SUM(update_count) FROM windowed_trip_aggregates GROUP BY is_final")
    top_windows = store._query(
        "SELECT window_start, window_end, location_id, trip_count, total_revenue, avg_fare_per_mile, update_count "
        "FROM windowed_trip_aggregates WHERE is_final ORDER BY trip_count DESC, window_start LIMIT 5")

    lines = [
        "=" * 78,
        "STREAMING DEMO SUMMARY",
        f"engine                      : {store.engine_type}",
        f"duration                    : {elapsed:.1f}s",
        f"produced / consumed         : {produced['count']} / {totals['consumed']} "
        f"({produced['count'] / max(elapsed, 0.001):.1f} events/s)",
        f"producer blocked (backpress): {produced['blocked_sec']:.1f}s",
        f"outcomes this run           : {outcomes}  duplicates={totals['duplicates']} malformed={totals['malformed']}",
        f"raw table by status         : {dict(status_counts)}",
        "window rows (is_final, rows, total upserts): " + str([tuple(r) for r in window_counts]),
        "top final windows:",
    ]
    for w in top_windows:
        lines.append(f"  {w[0]} -> {w[1]}  zone {w[2]:>3}  trips {w[3]:>3}  revenue ${float(w[4]):>8.2f}  "
                     f"fare/mile {float(w[5] or 0):.2f}  upserts {w[6]}")
    lines += ["metrics: http://localhost:8000/metrics", "=" * 78]
    print("\n".join(lines))


def main() -> None:
    parser = argparse.ArgumentParser(description="Streaming pipeline demo without Kafka")
    parser.add_argument("--events", type=int, default=1500)
    parser.add_argument("--rate", type=int, default=50, help="Events per second (10-50)")
    parser.add_argument("--month", default="2023-01")
    parser.add_argument("--late-ratio", type=float, default=0.05)
    parser.add_argument("--max-delay-events", type=int, default=400)
    parser.add_argument("--buffer-size", type=int, default=200, help="In-memory topic capacity")
    parser.add_argument("--slow-db-ms", type=int, default=0, help="Artificial delay per DB write")
    parser.add_argument("--duckdb", action="store_true", help="Force DuckDB storage")
    parser.add_argument("--fresh", action="store_true", help="Empty the streaming tables before running")
    args = parser.parse_args()
    setup_logging("streaming_demo")
    run_demo(args)


if __name__ == "__main__":
    main()
