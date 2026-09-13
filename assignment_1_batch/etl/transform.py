import logging
from typing import Dict, Iterable, Optional, Tuple

import pandas as pd

from assignment_1_batch.etl.extract import normalize_columns

logger = logging.getLogger("etl.transform")

VALID_VENDOR_IDS = {1, 2, 6, 7}
UNKNOWN_VENDOR_ID = 99
VALID_RATE_CODE_IDS = {1, 2, 3, 4, 5, 6, 99}
UNKNOWN_RATE_CODE_ID = 99
VALID_PAYMENT_TYPE_IDS = {0, 1, 2, 3, 4, 5, 6}
UNKNOWN_PAYMENT_TYPE_ID = 5
MIN_LOCATION_ID, MAX_LOCATION_ID = 1, 265
UNKNOWN_LOCATION_ID = 264

MAX_TRIP_DISTANCE_MILES = 150.0
MAX_FARE_AMOUNT = 2500.0
MIN_TRIP_DURATION_MIN = 0.5
MAX_TRIP_DURATION_MIN = 1440.0

COLUMN_MAPPING = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "RatecodeID": "rate_code_id",
    "PULocationID": "pulocation_id",
    "DOLocationID": "dolocation_id",
    "payment_type": "payment_type_id",
}

AMOUNT_COLUMNS = [
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "congestion_surcharge",
    "airport_fee",
]

FACT_COLUMNS = [
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "pickup_date_id",
    "dropoff_date_id",
    "pickup_hour",
    "pulocation_id",
    "dolocation_id",
    "rate_code_id",
    "payment_type_id",
    "store_and_fwd_flag",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "congestion_surcharge",
    "airport_fee",
    "total_amount",
    "trip_duration_minutes",
    "fare_per_mile",
]


def _coded(series: pd.Series, valid: Iterable[int], unknown: int) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    return values.where(values.isin(list(valid)), unknown).astype("int64")


def _date_id(timestamps: pd.Series) -> pd.Series:
    return (timestamps.dt.year * 10000 + timestamps.dt.month * 100 + timestamps.dt.day).astype("int64")


def clean_and_transform(
    df_raw: pd.DataFrame,
    period_start: Optional[pd.Timestamp] = None,
    period_end: Optional[pd.Timestamp] = None,
) -> Tuple[pd.DataFrame, Dict[str, float]]:
    initial_count = len(df_raw)
    df = normalize_columns(df_raw).rename(columns=COLUMN_MAPPING)
    df = df.loc[:, ~df.columns.duplicated()].copy()

    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], errors="coerce")
    df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"], errors="coerce")
    df["trip_distance"] = pd.to_numeric(df["trip_distance"], errors="coerce")
    df["fare_amount"] = pd.to_numeric(df["fare_amount"], errors="coerce")

    stats: Dict[str, float] = {"initial_raw_rows": initial_count}

    def drop_invalid(mask: pd.Series, stat_name: str) -> None:
        nonlocal df
        stats[stat_name] = int((~mask).sum())
        df = df[mask]

    if period_start is not None and period_end is not None:
        drop_invalid(
            (df["pickup_datetime"] >= period_start) & (df["pickup_datetime"] < period_end),
            "dropped_out_of_period",
        )
    else:
        stats["dropped_out_of_period"] = 0

    drop_invalid(
        (df["trip_distance"] > 0) & (df["trip_distance"] <= MAX_TRIP_DISTANCE_MILES), "dropped_invalid_distance"
    )
    drop_invalid((df["fare_amount"] > 0) & (df["fare_amount"] <= MAX_FARE_AMOUNT), "dropped_invalid_fare")
    drop_invalid(df["dropoff_datetime"] > df["pickup_datetime"], "dropped_invalid_temporal")

    duration_min = (df["dropoff_datetime"] - df["pickup_datetime"]).dt.total_seconds() / 60.0
    drop_invalid(
        (duration_min >= MIN_TRIP_DURATION_MIN) & (duration_min <= MAX_TRIP_DURATION_MIN), "dropped_invalid_duration"
    )

    df = df.copy()
    df["trip_duration_minutes"] = ((df["dropoff_datetime"] - df["pickup_datetime"]).dt.total_seconds() / 60.0).round(2)

    df["vendor_id"] = _coded(df.get("vendor_id"), VALID_VENDOR_IDS, UNKNOWN_VENDOR_ID)
    df["rate_code_id"] = _coded(df.get("rate_code_id"), VALID_RATE_CODE_IDS, UNKNOWN_RATE_CODE_ID)
    df["payment_type_id"] = _coded(df.get("payment_type_id"), VALID_PAYMENT_TYPE_IDS, UNKNOWN_PAYMENT_TYPE_ID)
    valid_locations = range(MIN_LOCATION_ID, MAX_LOCATION_ID + 1)
    df["pulocation_id"] = _coded(df["pulocation_id"], valid_locations, UNKNOWN_LOCATION_ID)
    df["dolocation_id"] = _coded(df["dolocation_id"], valid_locations, UNKNOWN_LOCATION_ID)

    passengers = pd.to_numeric(df.get("passenger_count"), errors="coerce")
    df["passenger_count"] = passengers.where(passengers.between(0, 9)).astype("Int64")

    if "store_and_fwd_flag" in df.columns:
        flag = df["store_and_fwd_flag"].astype("string").str.strip().str.upper()
        df["store_and_fwd_flag"] = flag.where(flag.isin(["Y", "N"]), "N").fillna("N").astype(str)
    else:
        df["store_and_fwd_flag"] = "N"

    for column in AMOUNT_COLUMNS:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0.0).round(2)
        else:
            df[column] = 0.0

    component_total = df[AMOUNT_COLUMNS].sum(axis=1).round(2)
    if "total_amount" in df.columns:
        df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce").fillna(component_total).round(2)
    else:
        df["total_amount"] = component_total

    df["pickup_date_id"] = _date_id(df["pickup_datetime"])
    df["dropoff_date_id"] = _date_id(df["dropoff_datetime"])
    df["pickup_hour"] = df["pickup_datetime"].dt.hour.astype("int64")
    df["fare_per_mile"] = (df["fare_amount"] / df["trip_distance"]).round(4)

    df_final = df[FACT_COLUMNS].reset_index(drop=True)

    stats["final_cleaned_rows"] = len(df_final)
    stats["rows_rejected"] = initial_count - len(df_final)
    stats["retention_rate_pct"] = round(len(df_final) * 100.0 / max(initial_count, 1), 2)
    logger.info("transform finished", extra=stats)
    return df_final, stats
