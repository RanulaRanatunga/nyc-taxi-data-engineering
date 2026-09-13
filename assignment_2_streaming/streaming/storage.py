import dataclasses
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Optional, Sequence, Set, Tuple

from assignment_2_streaming.streaming.config import STREAMING_SQL_DIR, streaming_db_config
from assignment_2_streaming.streaming.events import TripEvent
from assignment_2_streaming.streaming.windowing import EventOutcome
from common.db import POSTGRES, DatabaseConfig, get_db_connection, split_sql_statements

logger = logging.getLogger("streaming.storage")

RAW_COLUMNS = [
    "trip_id", "vendor_id", "pickup_datetime", "dropoff_datetime", "pulocation_id", "dolocation_id",
    "passenger_count", "trip_distance", "fare_amount", "tip_amount", "total_amount", "payment_type",
    "event_status", "produced_at", "ingested_at", "kafka_partition", "kafka_offset",
]
WINDOW_COLUMNS = [
    "window_start", "window_end", "location_id", "trip_count", "passenger_count", "total_fare", "total_tips",
    "total_revenue", "total_distance", "avg_fare_per_mile", "avg_trip_duration_min", "is_final", "last_updated_at",
]
_WINDOW_UPDATE_COLUMNS = [c for c in WINDOW_COLUMNS if c not in ("window_start", "window_end", "location_id")]
_AGGREGATED_STATUSES = (EventOutcome.ACCEPTED.value, EventOutcome.OUT_OF_ORDER.value)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class StreamingStore:
    def __init__(self, conn, engine_type: str, db_cfg: DatabaseConfig, write_delay_sec: float = 0.0):
        self.conn = conn
        self.engine_type = engine_type
        self.db_cfg = db_cfg
        self.write_delay_sec = write_delay_sec

    @classmethod
    def connect(cls, db_cfg: Optional[DatabaseConfig] = None, write_delay_sec: float = 0.0) -> "StreamingStore":
        db_cfg = db_cfg or streaming_db_config()
        conn, engine_type = get_db_connection(db_cfg)
        logger.info("streaming store connected", extra={"engine": engine_type})
        return cls(conn, engine_type, db_cfg, write_delay_sec)

    @property
    def _mark(self) -> str:
        return "%s" if self.engine_type == POSTGRES else "?"

    def reconnect(self) -> None:
        self.close()
        cfg = dataclasses.replace(self.db_cfg, engine_type=self.engine_type, allow_duckdb_fallback=False)
        self.conn, _ = get_db_connection(cfg)

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass

    def initialize_schema(self) -> None:
        ddl = (STREAMING_SQL_DIR / "streaming_schema.sql").read_text(encoding="utf-8")
        if self.engine_type == POSTGRES:
            with self.conn.cursor() as cursor:
                cursor.execute(ddl)
            self.conn.commit()
        else:
            for statement in split_sql_statements(ddl):
                self.conn.execute(statement)

    def truncate(self) -> None:
        self._execute_in_transaction(lambda cur: [
            cur.execute("DELETE FROM streaming_taxi_trips"),
            cur.execute("DELETE FROM windowed_trip_aggregates"),
        ])

    def _query(self, sql: str, params: Sequence = ()) -> List[tuple]:
        if self.engine_type == POSTGRES:
            with self.conn.cursor() as cursor:
                cursor.execute(sql, tuple(params))
                rows = cursor.fetchall()
            self.conn.commit()
            return rows
        return self.conn.execute(sql, list(params)).fetchall()

    def existing_trip_ids(self, trip_ids: Sequence[str]) -> Set[str]:
        if not trip_ids:
            return set()
        marks = ", ".join([self._mark] * len(trip_ids))
        rows = self._query(f"SELECT trip_id FROM streaming_taxi_trips WHERE trip_id IN ({marks})", trip_ids)
        return {row[0] for row in rows}

    def load_recent_aggregated_events(self, lookback_sec: int) -> List[TripEvent]:
        status_marks = ", ".join([self._mark] * len(_AGGREGATED_STATUSES))
        max_row = self._query(
            f"SELECT MAX(pickup_datetime) FROM streaming_taxi_trips WHERE event_status IN ({status_marks})",
            _AGGREGATED_STATUSES,
        )
        max_pickup = max_row[0][0] if max_row else None
        if max_pickup is None:
            return []
        since_dt = max_pickup - timedelta(seconds=lookback_sec)
        rows = self._query(
            "SELECT trip_id, pickup_datetime, dropoff_datetime, pulocation_id, dolocation_id, trip_distance, "
            "fare_amount, tip_amount, total_amount, vendor_id, passenger_count, payment_type "
            f"FROM streaming_taxi_trips WHERE event_status IN ({status_marks}) AND pickup_datetime >= {self._mark}",
            (*_AGGREGATED_STATUSES, since_dt),
        )
        return [
            TripEvent(
                trip_id=r[0], pickup_datetime=r[1], dropoff_datetime=r[2], pulocation_id=int(r[3]),
                dolocation_id=int(r[4]), trip_distance=float(r[5]), fare_amount=float(r[6]),
                tip_amount=float(r[7]), total_amount=float(r[8]), vendor_id=r[9], passenger_count=r[10],
                payment_type=r[11],
            )
            for r in rows
        ]

    def write_batch(self, events: Iterable[Tuple[TripEvent, EventOutcome]], window_rows: List[dict]) -> int:
        now = _utc_now()
        raw_rows = [
            (
                e.trip_id, e.vendor_id, e.pickup_datetime, e.dropoff_datetime, e.pulocation_id, e.dolocation_id,
                e.passenger_count, e.trip_distance, e.fare_amount, e.tip_amount, e.total_amount, e.payment_type,
                outcome.value,
                datetime.fromtimestamp(e.produced_at, tz=timezone.utc).replace(tzinfo=None) if e.produced_at else None,
                now, e.kafka_partition, e.kafka_offset,
            )
            for e, outcome in events
        ]
        agg_rows = [tuple(row[c] for c in WINDOW_COLUMNS[:-1]) + (now,) for row in window_rows]
        if not raw_rows and not agg_rows:
            return 0

        raw_sql = (f"INSERT INTO streaming_taxi_trips ({', '.join(RAW_COLUMNS)}) VALUES {{values}} "
                   "ON CONFLICT (trip_id) DO NOTHING")
        window_sql = (
            f"INSERT INTO windowed_trip_aggregates ({', '.join(WINDOW_COLUMNS)}) VALUES {{values}} "
            "ON CONFLICT (window_start, window_end, location_id) DO UPDATE SET "
            + ", ".join(f"{c} = EXCLUDED.{c}" for c in _WINDOW_UPDATE_COLUMNS)
            + ", update_count = windowed_trip_aggregates.update_count + 1"
        )

        def write(cursor) -> None:
            if self.write_delay_sec:
                time.sleep(self.write_delay_sec)
            if self.engine_type == POSTGRES:
                import psycopg2.extras

                if raw_rows:
                    psycopg2.extras.execute_values(cursor, raw_sql.format(values="%s"), raw_rows, page_size=1000)
                if agg_rows:
                    psycopg2.extras.execute_values(cursor, window_sql.format(values="%s"), agg_rows, page_size=1000)
            else:
                if raw_rows:
                    cursor.executemany(raw_sql.format(values=f"({', '.join('?' * len(RAW_COLUMNS))})"), raw_rows)
                if agg_rows:
                    cursor.executemany(window_sql.format(values=f"({', '.join('?' * len(WINDOW_COLUMNS))})"), agg_rows)

        self._execute_in_transaction(write)
        return len(raw_rows)

    def _execute_in_transaction(self, fn) -> None:
        if self.engine_type == POSTGRES:
            try:
                with self.conn.cursor() as cursor:
                    fn(cursor)
                self.conn.commit()
            except Exception:
                self._safe_rollback()
                raise
        else:
            self.conn.execute("BEGIN TRANSACTION")
            try:
                fn(self.conn)
                self.conn.execute("COMMIT")
            except Exception:
                self._safe_rollback()
                raise

    def _safe_rollback(self) -> None:
        try:
            if self.engine_type == POSTGRES:
                self.conn.rollback()
            else:
                self.conn.execute("ROLLBACK")
        except Exception:
            pass
