#!/usr/bin/env python3
import argparse
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

BASE_DIR = Path(__file__).resolve().parents[2]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

os.environ.setdefault("PREFECT_LOGGING_EXTRA_LOGGERS", "etl,common")

from prefect import flow, task

try:
    from prefect.cache_policies import NO_CACHE

    TASK_OPTIONS = {"cache_policy": NO_CACHE}
except ImportError:
    TASK_OPTIONS = {}

from assignment_1_batch.etl.alerts import flow_failure_hook, task_failure_hook
from assignment_1_batch.etl.config import (
    RAW_DATA_DIR,
    DatabaseConfig,
    get_db_connection,
    month_bounds,
    validate_month,
)
from assignment_1_batch.etl.extract import extract_parquet
from assignment_1_batch.etl.load import load_fact_table, log_audit_record
from assignment_1_batch.etl.transform import clean_and_transform
from assignment_1_batch.scripts.download_data import download_month_data
from common.logging_utils import setup_logging

logger = logging.getLogger("etl.pipeline")


def _current_flow_run_id() -> Optional[str]:
    try:
        from prefect.runtime import flow_run

        return str(flow_run.id) if flow_run.id else None
    except Exception:
        return None


@task(name="download-month", retries=3, retry_delay_seconds=10, on_failure=[task_failure_hook], **TASK_OPTIONS)
def download_task(month: str, allow_synthetic: bool) -> Path:
    return download_month_data(month, RAW_DATA_DIR, allow_synthetic_fallback=allow_synthetic)


@task(name="extract-parquet", retries=2, retry_delay_seconds=5, on_failure=[task_failure_hook], **TASK_OPTIONS)
def extract_task(file_path: Path, sample_limit: Optional[int]):
    return extract_parquet(file_path, sample_limit=sample_limit)


@task(name="transform-clean", on_failure=[task_failure_hook], **TASK_OPTIONS)
def transform_task(df_raw, month: str):
    period_start, period_end = month_bounds(month)
    return clean_and_transform(df_raw, period_start=period_start, period_end=period_end)


@task(name="load-star-schema", retries=1, retry_delay_seconds=10, on_failure=[task_failure_hook], **TASK_OPTIONS)
def load_task(df, month: str, batch_id: str) -> int:
    period_start, period_end = month_bounds(month)
    conn, engine_type = get_db_connection(DatabaseConfig.from_env())
    try:
        return load_fact_table(df, conn, engine_type, batch_id, period_start, period_end)
    finally:
        conn.close()


def _write_audit(record: dict) -> None:
    conn, engine_type = get_db_connection(DatabaseConfig.from_env())
    try:
        log_audit_record(conn, engine_type, record)
    finally:
        conn.close()


@flow(name="nyc-taxi-month-etl", on_failure=[flow_failure_hook], log_prints=True)
def month_etl_flow(month: str, sample_limit: Optional[int] = None, allow_synthetic: bool = False) -> dict:
    validate_month(month)
    batch_id = f"batch_{month}_{uuid.uuid4().hex[:8]}"
    started_at = datetime.now(timezone.utc).replace(tzinfo=None)
    start = time.time()
    record = {
        "batch_id": batch_id,
        "flow_run_id": _current_flow_run_id(),
        "source_file": f"yellow_tripdata_{month}.parquet",
        "stage": "DOWNLOAD",
        "rows_extracted": 0,
        "rows_cleaned": 0,
        "rows_rejected": 0,
        "rows_loaded": 0,
        "started_at": started_at,
    }
    logger.info("pipeline started", extra={"batch_id": batch_id, "month": month, "sample_limit": sample_limit})

    try:
        file_path = download_task(month, allow_synthetic)

        record["stage"] = "EXTRACT"
        df_raw, _ = extract_task(file_path, sample_limit)
        record["rows_extracted"] = len(df_raw)

        record["stage"] = "TRANSFORM"
        df_clean, stats = transform_task(df_raw, month)
        del df_raw
        record["rows_cleaned"] = len(df_clean)
        record["rows_rejected"] = stats["rows_rejected"]

        record["stage"] = "LOAD"
        record["rows_loaded"] = load_task(df_clean, month, batch_id)

        record["stage"] = "COMPLETED"
        record["status"] = "SUCCESS"
        return record
    except Exception as exc:
        record["status"] = "FAILED"
        record["error_message"] = f"{type(exc).__name__}: {exc}"[:2000]
        logger.error("pipeline failed", extra={"batch_id": batch_id, "stage": record["stage"]}, exc_info=True)
        raise
    finally:
        record["finished_at"] = datetime.now(timezone.utc).replace(tzinfo=None)
        record["execution_duration_sec"] = round(time.time() - start, 2)
        record.setdefault("status", "FAILED")
        _write_audit(record)
        logger.info(
            "pipeline finished",
            extra={key: record.get(key) for key in (
                "batch_id", "status", "stage", "rows_extracted", "rows_cleaned", "rows_rejected",
                "rows_loaded", "started_at", "finished_at", "execution_duration_sec",
            )},
        )


@flow(name="nyc-taxi-batch-etl", on_failure=[flow_failure_hook], log_prints=True)
def batch_etl_flow(
    months: List[str],
    sample_limit: Optional[int] = None,
    allow_synthetic: bool = False,
    fail_fast: bool = False,
) -> List[dict]:
    results, failed_months = [], []
    for month in months:
        try:
            results.append(month_etl_flow(month, sample_limit=sample_limit, allow_synthetic=allow_synthetic))
        except Exception:
            failed_months.append(month)
            if fail_fast:
                raise

    if failed_months:
        raise RuntimeError(f"Batch ETL failed for months: {failed_months}")
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="NYC Yellow Taxi batch ETL (Prefect)")
    parser.add_argument("--months", nargs="+", default=["2023-01", "2023-02"], help="Months to process (YYYY-MM)")
    parser.add_argument("--sample-limit", type=int, default=None,
                        help="Only read the first N rows of each file (default: full file)")
    parser.add_argument("--allow-synthetic", action="store_true",
                        help="Generate synthetic data if the TLC download is unavailable")
    parser.add_argument("--fail-fast", action="store_true", help="Stop at the first failing month")
    parser.add_argument("--serve", action="store_true",
                        help="Run as a long-lived Prefect deployment scheduled monthly (Ctrl+C to stop)")
    parser.add_argument("--cron", default="0 6 5 * *", help="Cron schedule used with --serve")
    args = parser.parse_args()

    setup_logging("batch_etl", console=False)
    for month in args.months:
        validate_month(month)

    parameters = {
        "months": args.months,
        "sample_limit": args.sample_limit,
        "allow_synthetic": args.allow_synthetic,
        "fail_fast": args.fail_fast,
    }
    if args.serve:
        batch_etl_flow.serve(name="nyc-taxi-monthly-batch", cron=args.cron, parameters=parameters)
        return

    try:
        batch_etl_flow(**parameters)
    except Exception:
        sys.exit(1)


if __name__ == "__main__":
    main()
