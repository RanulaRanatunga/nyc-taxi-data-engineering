from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from assignment_1_batch.etl.extract import validate_schema
from assignment_1_batch.etl.load import load_fact_table, log_audit_record
from assignment_1_batch.etl.queries import load_named_queries
from assignment_1_batch.etl.transform import FACT_COLUMNS, clean_and_transform
from assignment_1_batch.scripts.init_db import build_date_dimension, initialize_schema, seed_dimension_tables


def raw_trips(**overrides) -> pd.DataFrame:
    base = datetime(2023, 1, 15, 12, 0, 0)
    data = {
        "VendorID": [1, 2, 1, 1, 2, 1],
        "tpep_pickup_datetime": [base, base, base, base + timedelta(minutes=20), base, base],
        "tpep_dropoff_datetime": [
            base + timedelta(minutes=15),
            base + timedelta(minutes=10),
            base + timedelta(minutes=10),
            base,
            base + timedelta(seconds=10),
            base + timedelta(minutes=25),
        ],
        "passenger_count": [1, 2, 1, 1, 2, 3],
        "trip_distance": [3.0, -1.5, 2.0, 4.0, 0.1, 5.0],
        "RatecodeID": [1, 1, 1, 1, 1, 1],
        "PULocationID": [100, 100, 100, 100, 100, 142],
        "DOLocationID": [140, 140, 140, 140, 140, 236],
        "payment_type": [1, 1, 1, 1, 1, 2],
        "fare_amount": [12.0, 10.0, -5.0, 15.0, 3.0, 20.0],
        "extra": [0.0] * 6,
        "mta_tax": [0.5] * 6,
        "tip_amount": [2.0, 0, 0, 0, 0, 0],
        "tolls_amount": [0.0] * 6,
        "improvement_surcharge": [1.0] * 6,
        "total_amount": [15.5, 11.5, -3.5, 16.5, 4.5, 21.5],
    }
    data.update(overrides)
    return pd.DataFrame(data)


def test_schema_validation_success():
    assert validate_schema(raw_trips(), Path("test.parquet")) is True


def test_schema_validation_is_case_insensitive():
    df = raw_trips().rename(columns={"VendorID": "vendorid", "PULocationID": "PUlocationID"})
    assert validate_schema(df, Path("test.parquet")) is True


def test_schema_validation_failure():
    with pytest.raises(ValueError) as exc:
        validate_schema(pd.DataFrame({"VendorID": [1], "passenger_count": [1]}), Path("bad.parquet"))
    assert "Missing required columns" in str(exc.value)


def test_data_cleaning_rules():
    df_cleaned, stats = clean_and_transform(raw_trips())

    assert len(df_cleaned) == 2
    assert stats["dropped_invalid_distance"] == 1
    assert stats["dropped_invalid_fare"] == 1
    assert stats["dropped_invalid_temporal"] == 1
    assert stats["dropped_invalid_duration"] == 1
    assert stats["rows_rejected"] == 4
    assert list(df_cleaned.columns) == FACT_COLUMNS
    assert df_cleaned["pickup_date_id"].iloc[0] == 20230115
    assert df_cleaned["pickup_hour"].iloc[0] == 12
    assert df_cleaned["fare_per_mile"].tolist() == [4.0, 4.0]


def test_rows_outside_source_month_are_dropped():
    df = raw_trips(tpep_pickup_datetime=[datetime(2008, 12, 31, 23)] + [datetime(2023, 1, 15, 12)] * 5)
    _, stats = clean_and_transform(df, pd.Timestamp("2023-01-01"), pd.Timestamp("2023-02-01"))
    assert stats["dropped_out_of_period"] == 1


def test_codes_nulls_and_unknown_members():
    df = raw_trips(
        VendorID=[5, 2, 1, 1, 2, 1],
        RatecodeID=[None, 1, 1, 1, 1, 99],
        payment_type=[0, 1, 1, 1, 1, 9],
        passenger_count=[None, 2, 1, 1, 2, 12],
        PULocationID=[300, 100, 100, 100, 100, 142],
    )
    cleaned, _ = clean_and_transform(df)
    first, last = cleaned.iloc[0], cleaned.iloc[1]
    assert first["vendor_id"] == 99
    assert first["rate_code_id"] == 99
    assert first["payment_type_id"] == 0
    assert pd.isna(first["passenger_count"])
    assert first["pulocation_id"] == 264
    assert last["payment_type_id"] == 5
    assert pd.isna(last["passenger_count"])


def test_date_dimension_flags():
    dim = build_date_dimension("2023-01-01", "2023-01-02").set_index("date_id")
    assert bool(dim.loc[20230101, "is_weekend"]) is True
    assert bool(dim.loc[20230102, "is_holiday"]) is True
    assert dim.loc[20230102, "day_of_week"] == 1


@pytest.fixture()
def warehouse(tmp_path):
    lookup = tmp_path / "taxi_zone_lookup.csv"
    rows = ["LocationID,Borough,Zone,service_zone"]
    rows += [f"{i},Manhattan,Zone {i},Yellow Zone" for i in range(1, 264)]
    rows += ['264,Unknown,N/A,N/A', '265,N/A,Outside of NYC,N/A']
    lookup.write_text("\n".join(rows), encoding="utf-8")

    conn = duckdb.connect()
    initialize_schema(conn, "duckdb")
    seed_dimension_tables(conn, "duckdb", lookup_file=lookup)
    yield conn
    conn.close()


def test_load_is_idempotent_and_queries_run(warehouse):
    cleaned, _ = clean_and_transform(raw_trips(), pd.Timestamp("2023-01-01"), pd.Timestamp("2023-02-01"))
    period = (pd.Timestamp("2023-01-01"), pd.Timestamp("2023-02-01"))

    load_fact_table(cleaned, warehouse, "duckdb", "batch_1", *period)
    load_fact_table(cleaned, warehouse, "duckdb", "batch_2", *period)
    assert warehouse.execute("SELECT COUNT(*), MIN(batch_id) FROM fact_taxi_trips").fetchone() == (2, "batch_2")

    log_audit_record(warehouse, "duckdb", {
        "batch_id": "batch_2", "source_file": "x.parquet", "stage": "COMPLETED", "status": "SUCCESS",
        "rows_extracted": 6, "rows_cleaned": 2, "rows_rejected": 4, "rows_loaded": 2,
        "started_at": datetime(2023, 1, 1), "finished_at": datetime(2023, 1, 1, 0, 1), "execution_duration_sec": 60,
    })

    queries = load_named_queries()
    assert {"avg_fare_per_mile", "peak_hours", "revenue_by_payment_type", "recent_etl_runs"} <= set(queries)
    results = {name: warehouse.execute(sql).fetchall() for name, sql in queries.items()}

    total_trips, weighted, simple = results["avg_fare_per_mile"][0]
    assert total_trips == 2
    assert float(weighted) == 4.0
    assert float(simple) == 4.0
    assert [row[0] for row in results["peak_hours"]] == [12]
    revenue = {row[1]: float(row[6]) for row in results["revenue_by_payment_type"]}
    assert revenue == {"Credit card": 15.5, "Cash": 21.5}
    assert len(results["recent_etl_runs"]) == 1
