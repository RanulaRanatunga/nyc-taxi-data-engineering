import json
from datetime import datetime, timedelta

import duckdb
import pytest

from assignment_2_streaming.streaming.events import InvalidEventError, TripEvent
from assignment_2_streaming.streaming.processor import StreamProcessor
from assignment_2_streaming.streaming.producer import OutOfOrderInjector, build_event
from assignment_2_streaming.streaming.storage import StreamingStore
from assignment_2_streaming.streaming.windowing import EventOutcome, SlidingWindowAggregator
from common.db import DatabaseConfig

BASE = datetime(2023, 1, 1, 10, 0, 0)


def trip(trip_id: str, pickup: datetime, location: int = 161, fare: float = 10.0, distance: float = 2.0,
         passengers: int = 1) -> TripEvent:
    return TripEvent(
        trip_id=trip_id, pickup_datetime=pickup, dropoff_datetime=pickup + timedelta(minutes=10),
        pulocation_id=location, dolocation_id=1, trip_distance=distance, fare_amount=fare,
        tip_amount=2.0, total_amount=fare + 2.0, passenger_count=passengers,
    )


def aggregator(lateness_sec: int = 120) -> SlidingWindowAggregator:
    return SlidingWindowAggregator(window_size_sec=300, slide_sec=60, allowed_lateness_sec=lateness_sec)


def test_event_belongs_to_all_overlapping_windows():
    agg = aggregator()
    event_ts = trip("T", BASE + timedelta(seconds=30)).event_ts
    starts = agg.window_starts(event_ts)
    assert len(starts) == 5
    assert all(start <= event_ts < start + 300 for start in starts)


def test_sliding_window_aggregation():
    agg = aggregator()
    assert agg.add_event(trip("T1", BASE, fare=10, distance=2, passengers=1)) is EventOutcome.ACCEPTED
    assert agg.add_event(trip("T2", BASE + timedelta(seconds=30), fare=15, distance=3, passengers=2)) \
        is EventOutcome.ACCEPTED

    rows = agg.flush(shutdown=True)
    assert len(rows) == 5
    for row in rows:
        assert row["location_id"] == 161
        assert row["trip_count"] == 2
        assert row["passenger_count"] == 3
        assert row["total_fare"] == 25.0
        assert row["total_revenue"] == 29.0
        assert row["avg_fare_per_mile"] == 5.0
        assert row["is_final"] is False
    assert agg.windows == {}


def test_out_of_order_event_within_watermark_is_aggregated():
    agg = aggregator(lateness_sec=120)
    now = trip("NOW", BASE)
    agg.add_event(now)
    outcome = agg.add_event(trip("SLIGHTLY_LATE", BASE - timedelta(seconds=90)))
    assert outcome is EventOutcome.OUT_OF_ORDER
    assert agg.max_event_ts == now.event_ts


def test_event_behind_watermark_is_late():
    agg = aggregator(lateness_sec=60)
    agg.add_event(trip("NOW", datetime(2023, 1, 1, 12, 0)))
    assert agg.add_event(trip("LATE", datetime(2023, 1, 1, 11, 50))) is EventOutcome.LATE
    assert agg.add_event(trip("PARTIAL", datetime(2023, 1, 1, 11, 57))) is EventOutcome.OUT_OF_ORDER


def test_windows_are_finalised_when_watermark_passes():
    agg = aggregator(lateness_sec=60)
    agg.add_event(trip("A", BASE))
    first = agg.flush()
    assert len(first) == 5 and not any(row["is_final"] for row in first)

    agg.add_event(trip("B", BASE + timedelta(minutes=10), location=50))
    rows = agg.flush()
    final = [row for row in rows if row["is_final"]]
    assert len(final) == 5 and {row["location_id"] for row in final} == {161}
    assert all(key[1] == 50 for key in agg.windows)


def test_future_clock_error_does_not_advance_watermark_until_confirmed():
    agg = SlidingWindowAggregator(300, 60, 120, max_future_skew_sec=3600, future_jump_threshold=3)
    agg.add_event(trip("A", BASE))
    future = BASE + timedelta(days=30)
    assert agg.add_event(trip("F1", future)) is EventOutcome.FUTURE
    assert agg.add_event(trip("B", BASE + timedelta(seconds=5))) is EventOutcome.ACCEPTED
    assert agg.add_event(trip("F2", future)) is EventOutcome.FUTURE
    assert agg.add_event(trip("F3", future)) is EventOutcome.FUTURE
    assert agg.add_event(trip("F4", future)) is EventOutcome.ACCEPTED


def test_rehydrate_restores_open_windows_only():
    agg = aggregator(lateness_sec=60)
    events = [trip("A", BASE), trip("B", BASE + timedelta(minutes=10))]
    agg.rehydrate(events)
    assert {key[0] for key in agg.windows} == {start for start in agg.window_starts(events[1].event_ts)}
    assert agg.flush() == []


def test_invalid_window_configuration():
    with pytest.raises(ValueError):
        SlidingWindowAggregator(window_size_sec=300, slide_sec=70)


def test_trip_event_validation():
    payload = TripEvent.to_dict(trip("T1", BASE))
    assert TripEvent.from_json(json.dumps(payload).encode()).trip_id == "T1"

    with pytest.raises(InvalidEventError):
        TripEvent.from_json(b"not json")
    with pytest.raises(InvalidEventError):
        TripEvent.from_dict({**payload, "pickup_datetime": "yesterday"})
    with pytest.raises(InvalidEventError):
        TripEvent.from_dict({**payload, "dropoff_datetime": (BASE - timedelta(hours=1)).isoformat()})
    with pytest.raises(InvalidEventError):
        TripEvent.from_dict({k: v for k, v in payload.items() if k != "fare_amount"})


def test_build_event_handles_missing_values():
    row = {
        "file_row_number": 7, "vendor_id": 2, "pickup_datetime": BASE, "dropoff_datetime": BASE + timedelta(minutes=5),
        "pulocation_id": 48, "dolocation_id": 238, "passenger_count": None, "trip_distance": 1.2,
        "payment_type": 0, "fare_amount": 9.3, "tip_amount": float("nan"), "total_amount": 14.3,
    }
    event = build_event(row, "yellow_tripdata_2023-01")
    assert event["trip_id"] == "yellow_tripdata_2023-01-7"
    assert event["passenger_count"] is None and event["tip_amount"] is None
    parsed = TripEvent.from_dict(event)
    assert parsed.tip_amount == 0.0


def test_out_of_order_injector_keeps_every_event():
    injector = OutOfOrderInjector(ratio=0.3, max_delay_events=10, seed=1)
    emitted = []
    for i in range(200):
        emitted.extend(injector.push({"seq": i}))
    emitted.extend(injector.drain())
    order = [event["seq"] for event in emitted]
    assert sorted(order) == list(range(200))
    assert order != list(range(200))


@pytest.fixture()
def store():
    conn = duckdb.connect()
    streaming_store = StreamingStore(conn, "duckdb", DatabaseConfig(engine_type="duckdb"))
    streaming_store.initialize_schema()
    yield streaming_store
    conn.close()


def test_processor_is_idempotent_and_upserts(store):
    processor = StreamProcessor(store, aggregator(lateness_sec=60))
    batch = [trip("T1", BASE), trip("T2", BASE + timedelta(seconds=20))]

    first = processor.process(batch + [batch[0]])
    assert first.duplicates == 1 and first.raw_written == 2

    redelivered = processor.process(batch)
    assert redelivered.duplicates == 2 and redelivered.raw_written == 0
    counts = store._query("SELECT DISTINCT trip_count FROM windowed_trip_aggregates")
    assert counts == [(2,)]

    processor.process([trip("T3", BASE + timedelta(seconds=40))])
    row = store._query("SELECT trip_count, update_count FROM windowed_trip_aggregates "
                       "ORDER BY window_start DESC LIMIT 1")[0]
    assert row == (3, 2)

    processor.process([trip("LATER", BASE + timedelta(minutes=15), location=7)])
    finals = store._query("SELECT COUNT(*) FROM windowed_trip_aggregates WHERE is_final AND location_id = 161")
    assert finals == [(5,)]


def test_restart_rebuilds_state_from_database(store):
    processor = StreamProcessor(store, aggregator(lateness_sec=120))
    processor.process([trip("T1", BASE), trip("T2", BASE + timedelta(seconds=10))])

    restarted = StreamProcessor(store, aggregator(lateness_sec=120))
    assert restarted.rehydrate() == 2
    restarted.process([trip("T3", BASE + timedelta(seconds=20))])
    counts = store._query("SELECT DISTINCT trip_count FROM windowed_trip_aggregates")
    assert counts == [(3,)]

    statuses = dict(store._query("SELECT event_status, COUNT(*) FROM streaming_taxi_trips GROUP BY 1"))
    assert statuses == {"accepted": 3}
