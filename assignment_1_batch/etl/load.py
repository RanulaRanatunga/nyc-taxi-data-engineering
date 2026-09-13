import io
import logging
import time
from typing import Optional

import pandas as pd

from common.db import POSTGRES, placeholder

logger = logging.getLogger("etl.load")

AUDIT_COLUMNS = [
    "batch_id",
    "flow_run_id",
    "source_file",
    "stage",
    "status",
    "rows_extracted",
    "rows_cleaned",
    "rows_rejected",
    "rows_loaded",
    "started_at",
    "finished_at",
    "execution_duration_sec",
    "error_message",
]


def load_fact_table(
    df: pd.DataFrame,
    conn,
    engine_type: str,
    batch_id: str,
    period_start: Optional[pd.Timestamp] = None,
    period_end: Optional[pd.Timestamp] = None,
) -> int:
    start_time = time.time()
    df = df.assign(batch_id=batch_id)
    columns = ", ".join(df.columns)
    replace_period = period_start is not None and period_end is not None

    if engine_type == POSTGRES:
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=False, date_format="%Y-%m-%d %H:%M:%S")
        buffer.seek(0)
        try:
            with conn.cursor() as cursor:
                deleted = 0
                if replace_period:
                    cursor.execute(
                        "DELETE FROM fact_taxi_trips WHERE pickup_datetime >= %s AND pickup_datetime < %s",
                        (period_start.to_pydatetime(), period_end.to_pydatetime()),
                    )
                    deleted = cursor.rowcount
                cursor.copy_expert(f"COPY fact_taxi_trips ({columns}) FROM STDIN WITH (FORMAT csv)", buffer)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    else:
        conn.execute("BEGIN TRANSACTION")
        try:
            deleted = 0
            if replace_period:
                deleted = conn.execute(
                    "DELETE FROM fact_taxi_trips WHERE pickup_datetime >= ? AND pickup_datetime < ?",
                    [period_start.to_pydatetime(), period_end.to_pydatetime()],
                ).fetchone()[0]
            conn.register("fact_batch_df", df)
            conn.execute(f"INSERT INTO fact_taxi_trips ({columns}) SELECT {columns} FROM fact_batch_df")
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.unregister("fact_batch_df")

    duration = time.time() - start_time
    logger.info(
        "load finished",
        extra={
            "engine": engine_type,
            "batch_id": batch_id,
            "rows_loaded": len(df),
            "rows_replaced": deleted,
            "duration_sec": round(duration, 2),
            "rows_per_sec": round(len(df) / max(duration, 0.001)),
        },
    )
    return len(df)


def log_audit_record(conn, engine_type: str, record: dict) -> None:
    values = tuple(record.get(column) for column in AUDIT_COLUMNS)
    marks = ", ".join([placeholder(engine_type)] * len(AUDIT_COLUMNS))
    sql = f"INSERT INTO etl_audit_log ({', '.join(AUDIT_COLUMNS)}) VALUES ({marks})"
    try:
        if engine_type == POSTGRES:
            conn.rollback()
            with conn.cursor() as cursor:
                cursor.execute(sql, values)
            conn.commit()
        else:
            conn.execute(sql, list(values))
    except Exception as exc:
        logger.error("failed to write audit record", extra={"batch_id": record.get("batch_id"), "error": str(exc)})
