#!/usr/bin/env python3
import argparse
import logging
import sys
from pathlib import Path

import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar

BASE_DIR = Path(__file__).resolve().parents[2]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from assignment_1_batch.etl.config import SQL_DIR, ZONE_LOOKUP_FILE, DatabaseConfig, get_db_connection
from assignment_1_batch.scripts.download_data import download_taxi_zone_lookup
from common.db import POSTGRES, split_sql_statements

logger = logging.getLogger("etl.init_db")

DATE_DIM_START, DATE_DIM_END = "2015-01-01", "2030-12-31"

TABLES_IN_DROP_ORDER = [
    "fact_taxi_trips", "dim_date", "dim_datetime", "dim_location",
    "dim_payment_type", "dim_rate_code", "dim_vendor", "etl_audit_log",
]

VENDORS = [
    (1, "Creative Mobile Technologies, LLC"),
    (2, "Curb Mobility, LLC"),
    (6, "Myle Technologies Inc"),
    (7, "Helix"),
    (99, "Unknown"),
]
RATE_CODES = [
    (1, "Standard rate"),
    (2, "JFK"),
    (3, "Newark"),
    (4, "Nassau or Westchester"),
    (5, "Negotiated fare"),
    (6, "Group ride"),
    (99, "Null/unknown"),
]
PAYMENT_TYPES = [
    (0, "Flex Fare trip", "Flex fare trip"),
    (1, "Credit card", "Paid by credit or debit card"),
    (2, "Cash", "Paid in cash"),
    (3, "No charge", "No charge"),
    (4, "Dispute", "Disputed fare"),
    (5, "Unknown", "Unknown or invalid payment type"),
    (6, "Voided trip", "Voided trip"),
]


def duckdb_ddl(sql: str) -> list:
    sql = sql.replace("BIGSERIAL PRIMARY KEY", "BIGINT PRIMARY KEY DEFAULT nextval('fact_trip_seq')")
    sql = sql.replace("SERIAL PRIMARY KEY", "INTEGER PRIMARY KEY DEFAULT nextval('etl_audit_seq')")
    statements = ["CREATE SEQUENCE IF NOT EXISTS fact_trip_seq", "CREATE SEQUENCE IF NOT EXISTS etl_audit_seq"]
    statements += [s for s in split_sql_statements(sql) if not s.upper().startswith("CREATE INDEX")]
    return statements


def reset_schema(conn, engine_type: str) -> None:
    logger.warning("dropping all star schema tables")
    if engine_type == POSTGRES:
        with conn.cursor() as cursor:
            for table in TABLES_IN_DROP_ORDER:
                cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        conn.commit()
    else:
        for table in TABLES_IN_DROP_ORDER:
            conn.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        conn.execute("DROP SEQUENCE IF EXISTS fact_trip_seq")
        conn.execute("DROP SEQUENCE IF EXISTS etl_audit_seq")


def initialize_schema(conn, engine_type: str) -> None:
    sql = (SQL_DIR / "schema.sql").read_text(encoding="utf-8")
    if engine_type == POSTGRES:
        with conn.cursor() as cursor:
            cursor.execute(sql)
        conn.commit()
    else:
        for statement in duckdb_ddl(sql):
            conn.execute(statement)
    logger.info("schema ready", extra={"engine": engine_type})


def build_date_dimension(start: str = DATE_DIM_START, end: str = DATE_DIM_END) -> pd.DataFrame:
    dates = pd.date_range(start=start, end=end, freq="D")
    holidays = set(USFederalHolidayCalendar().holidays(start=start, end=end))
    return pd.DataFrame({
        "date_id": (dates.year * 10000 + dates.month * 100 + dates.day).astype("int64"),
        "full_date": dates.date,
        "year": dates.year.astype("int64"),
        "quarter": dates.quarter.astype("int64"),
        "month": dates.month.astype("int64"),
        "month_name": dates.strftime("%B"),
        "day_of_month": dates.day.astype("int64"),
        "day_of_week": (dates.dayofweek + 1).astype("int64"),
        "day_name": dates.strftime("%A"),
        "week_of_year": dates.isocalendar().week.astype("int64").to_numpy(),
        "is_weekend": dates.dayofweek >= 5,
        "is_holiday": [d in holidays for d in dates],
    })


def build_location_dimension(lookup_file: Path) -> pd.DataFrame:
    df = pd.read_csv(lookup_file, keep_default_na=False)
    df = df.rename(columns={"LocationID": "location_id", "Borough": "borough", "Zone": "zone"})
    df["location_id"] = df["location_id"].astype("int64")
    for column in ("borough", "zone", "service_zone"):
        df[column] = df[column].replace("", "Unknown").astype(str)
    return df[["location_id", "borough", "zone", "service_zone"]]


def _insert_rows(conn, engine_type: str, table: str, df: pd.DataFrame) -> None:
    columns = ", ".join(df.columns)
    if engine_type == POSTGRES:
        import psycopg2.extras

        rows = list(df.astype(object).itertuples(index=False, name=None))
        with conn.cursor() as cursor:
            psycopg2.extras.execute_values(
                cursor, f"INSERT INTO {table} ({columns}) VALUES %s ON CONFLICT DO NOTHING", rows
            )
    else:
        conn.register("seed_df", df)
        try:
            conn.execute(f"INSERT INTO {table} ({columns}) SELECT {columns} FROM seed_df ON CONFLICT DO NOTHING")
        finally:
            conn.unregister("seed_df")


def seed_dimension_tables(conn, engine_type: str, lookup_file: Path = None) -> None:
    lookup_file = lookup_file or download_taxi_zone_lookup(ZONE_LOOKUP_FILE)
    seeds = {
        "dim_vendor": pd.DataFrame(VENDORS, columns=["vendor_id", "vendor_name"]),
        "dim_rate_code": pd.DataFrame(RATE_CODES, columns=["rate_code_id", "rate_description"]),
        "dim_payment_type": pd.DataFrame(PAYMENT_TYPES, columns=["payment_type_id", "payment_name", "description"]),
        "dim_date": build_date_dimension(),
        "dim_location": build_location_dimension(lookup_file),
    }
    for table, df in seeds.items():
        _insert_rows(conn, engine_type, table, df)
        logger.info("dimension seeded", extra={"table": table, "rows": len(df)})
    if engine_type == POSTGRES:
        conn.commit()


def main() -> None:
    from common.logging_utils import setup_logging

    parser = argparse.ArgumentParser(description="Initialise the NYC taxi star schema")
    parser.add_argument("--reset", action="store_true", help="Drop and recreate all tables (deletes loaded data)")
    args = parser.parse_args()

    setup_logging("init_db")
    conn, engine_type = get_db_connection(DatabaseConfig.from_env())
    try:
        if args.reset:
            reset_schema(conn, engine_type)
        initialize_schema(conn, engine_type)
        seed_dimension_tables(conn, engine_type)
        logger.info("database initialised", extra={"engine": engine_type})
    finally:
        conn.close()


if __name__ == "__main__":
    main()
