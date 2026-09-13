import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, Iterable, List, Optional, Set, Tuple

from assignment_2_streaming.streaming.events import TripEvent

logger = logging.getLogger("streaming.windowing")


class EventOutcome(str, Enum):
    ACCEPTED = "accepted"
    OUT_OF_ORDER = "out_of_order"
    LATE = "late"
    FUTURE = "future"

    @property
    def aggregated(self) -> bool:
        return self in (EventOutcome.ACCEPTED, EventOutcome.OUT_OF_ORDER)


@dataclass
class WindowAggregate:
    trip_count: int = 0
    passenger_count: int = 0
    total_fare: float = 0.0
    total_tips: float = 0.0
    total_revenue: float = 0.0
    total_distance: float = 0.0
    total_duration_min: float = 0.0

    def add(self, event: TripEvent) -> None:
        self.trip_count += 1
        self.passenger_count += event.passenger_count or 0
        self.total_fare += event.fare_amount
        self.total_tips += event.tip_amount
        self.total_revenue += event.total_amount
        self.total_distance += event.trip_distance
        self.total_duration_min += event.duration_min


def _utc_naive(ts: float) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc).replace(tzinfo=None)


class SlidingWindowAggregator:
    def __init__(
        self,
        window_size_sec: int = 300,
        slide_sec: int = 60,
        allowed_lateness_sec: int = 120,
        max_future_skew_sec: int = 3600,
        future_jump_threshold: int = 50,
    ):
        if window_size_sec <= 0 or slide_sec <= 0 or window_size_sec % slide_sec != 0:
            raise ValueError("window_size_sec must be a positive multiple of slide_sec")
        self.window_size_sec = window_size_sec
        self.slide_sec = slide_sec
        self.allowed_lateness_sec = allowed_lateness_sec
        self.max_future_skew_sec = max_future_skew_sec
        self.future_jump_threshold = future_jump_threshold

        self.max_event_ts: Optional[float] = None
        self.windows: Dict[Tuple[int, int], WindowAggregate] = {}
        self._dirty: Set[Tuple[int, int]] = set()
        self._consecutive_future = 0

    @classmethod
    def from_config(cls, cfg) -> "SlidingWindowAggregator":
        return cls(cfg.window_size_sec, cfg.slide_sec, cfg.allowed_lateness_sec, cfg.max_future_skew_sec)

    @property
    def watermark(self) -> Optional[float]:
        return None if self.max_event_ts is None else self.max_event_ts - self.allowed_lateness_sec

    @property
    def state_lookback_sec(self) -> int:
        return self.window_size_sec + self.allowed_lateness_sec

    def window_starts(self, event_ts: float) -> List[int]:
        last_start = int(event_ts // self.slide_sec) * self.slide_sec
        first_start = last_start - self.window_size_sec + self.slide_sec
        return list(range(first_start, last_start + 1, self.slide_sec))

    def add_event(self, event: TripEvent) -> EventOutcome:
        ts = event.event_ts

        if self.max_event_ts is not None and ts > self.max_event_ts + self.max_future_skew_sec:
            self._consecutive_future += 1
            if self._consecutive_future < self.future_jump_threshold:
                return EventOutcome.FUTURE
            logger.warning("event time jumped forward, accepting new clock",
                           extra={"previous_max": _utc_naive(self.max_event_ts), "new_event_time": _utc_naive(ts)})
        self._consecutive_future = 0

        watermark = self.watermark
        open_starts = [s for s in self.window_starts(ts) if watermark is None or s + self.window_size_sec > watermark]
        if not open_starts:
            return EventOutcome.LATE

        for start in open_starts:
            key = (start, event.pulocation_id)
            self.windows.setdefault(key, WindowAggregate()).add(event)
            self._dirty.add(key)

        if self.max_event_ts is None or ts >= self.max_event_ts:
            self.max_event_ts = ts
            return EventOutcome.ACCEPTED
        return EventOutcome.OUT_OF_ORDER

    def flush(self, shutdown: bool = False) -> List[dict]:
        watermark = self.watermark
        rows, evict = [], []
        for key, aggregate in self.windows.items():
            start, _ = key
            closed = watermark is not None and start + self.window_size_sec <= watermark
            if closed or shutdown or key in self._dirty:
                rows.append(self._to_row(key, aggregate, is_final=closed))
            if closed or shutdown:
                evict.append(key)
        for key in evict:
            del self.windows[key]
        self._dirty.clear()
        return rows

    def rehydrate(self, events: Iterable[TripEvent]) -> int:
        self.windows.clear()
        self._dirty.clear()
        self.max_event_ts = None
        self._consecutive_future = 0
        count = 0
        for event in sorted(events, key=lambda e: e.event_ts):
            self.add_event(event)
            count += 1
        watermark = self.watermark
        if watermark is not None:
            for key in [k for k in self.windows if k[0] + self.window_size_sec <= watermark]:
                del self.windows[key]
        self._dirty.clear()
        return count

    def _to_row(self, key: Tuple[int, int], agg: WindowAggregate, is_final: bool) -> dict:
        start, location_id = key
        return {
            "window_start": _utc_naive(start),
            "window_end": _utc_naive(start + self.window_size_sec),
            "location_id": location_id,
            "trip_count": agg.trip_count,
            "passenger_count": agg.passenger_count,
            "total_fare": round(agg.total_fare, 2),
            "total_tips": round(agg.total_tips, 2),
            "total_revenue": round(agg.total_revenue, 2),
            "total_distance": round(agg.total_distance, 2),
            "avg_fare_per_mile": round(agg.total_fare / agg.total_distance, 4) if agg.total_distance > 0 else None,
            "avg_trip_duration_min": round(agg.total_duration_min / agg.trip_count, 2) if agg.trip_count else None,
            "is_final": is_final,
        }
