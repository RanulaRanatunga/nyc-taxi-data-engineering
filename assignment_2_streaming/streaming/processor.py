import logging
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import List

from assignment_2_streaming.streaming import metrics
from assignment_2_streaming.streaming.events import TripEvent
from assignment_2_streaming.streaming.storage import StreamingStore
from assignment_2_streaming.streaming.windowing import SlidingWindowAggregator

logger = logging.getLogger("streaming.processor")


@dataclass
class BatchResult:
    received: int = 0
    duplicates: int = 0
    outcomes: Counter = field(default_factory=Counter)
    raw_written: int = 0
    windows_upserted: int = 0
    write_seconds: float = 0.0


class StreamProcessor:
    def __init__(self, store: StreamingStore, aggregator: SlidingWindowAggregator):
        self.store = store
        self.aggregator = aggregator

    def rehydrate(self) -> int:
        events = self.store.load_recent_aggregated_events(self.aggregator.state_lookback_sec)
        count = self.aggregator.rehydrate(events)
        self._update_state_metrics()
        logger.info("window state rebuilt from database",
                    extra={"events_replayed": count, "open_windows": len(self.aggregator.windows)})
        return count

    def process(self, events: List[TripEvent]) -> BatchResult:
        batch_start = time.perf_counter()
        result = BatchResult(received=len(events))

        unique = {}
        for event in events:
            unique.setdefault(event.trip_id, event)
        existing = self.store.existing_trip_ids(list(unique))
        fresh = [event for trip_id, event in unique.items() if trip_id not in existing]
        result.duplicates = len(events) - len(fresh)

        scored = [(event, self.aggregator.add_event(event)) for event in fresh]
        result.outcomes = Counter(outcome.value for _, outcome in scored)
        window_rows = self.aggregator.flush()

        write_start = time.perf_counter()
        result.raw_written = self.store.write_batch(scored, window_rows)
        result.write_seconds = time.perf_counter() - write_start
        result.windows_upserted = len(window_rows)

        committed_at = time.time()
        for event, _ in scored:
            if event.produced_at:
                metrics.END_TO_END_LATENCY.observe(max(0.0, committed_at - event.produced_at))
        for status, count in result.outcomes.items():
            metrics.EVENTS_PROCESSED.labels(status=status).inc(count)
        if result.duplicates:
            metrics.EVENTS_PROCESSED.labels(status="duplicate").inc(result.duplicates)
        final_rows = sum(1 for row in window_rows if row["is_final"])
        metrics.WINDOW_UPSERTS.labels(final="true").inc(final_rows)
        metrics.WINDOW_UPSERTS.labels(final="false").inc(len(window_rows) - final_rows)
        metrics.RAW_EVENTS_PERSISTED.inc(result.raw_written)
        metrics.DB_WRITE_SECONDS.observe(result.write_seconds)
        metrics.BATCH_PROCESSING_SECONDS.observe(time.perf_counter() - batch_start)
        self._update_state_metrics()
        return result

    def shutdown_flush(self) -> int:
        rows = self.aggregator.flush(shutdown=True)
        self.store.write_batch([], rows)
        self._update_state_metrics()
        return len(rows)

    def _update_state_metrics(self) -> None:
        metrics.OPEN_WINDOWS.set(len(self.aggregator.windows))
        if self.aggregator.watermark is not None:
            metrics.WATERMARK_TIMESTAMP.set(self.aggregator.watermark)
