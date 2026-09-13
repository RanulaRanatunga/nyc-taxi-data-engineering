import re
from typing import Tuple

import pandas as pd

from common.db import DatabaseConfig, get_db_connection
from common.settings import BASE_DIR, DATA_DIR, RAW_DATA_DIR

__all__ = [
    "DatabaseConfig", "get_db_connection", "BASE_DIR", "DATA_DIR", "RAW_DATA_DIR", "SQL_DIR", "TLC_BASE_URL",
    "ZONE_LOOKUP_URL", "ZONE_LOOKUP_FILE", "validate_month", "month_bounds", "raw_file_for_month",
]

SQL_DIR = BASE_DIR / "assignment_1_batch" / "sql"
TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
ZONE_LOOKUP_FILE = DATA_DIR / "taxi_zone_lookup.csv"

_MONTH_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def validate_month(month: str) -> str:
    if not _MONTH_PATTERN.match(month):
        raise ValueError(f"Invalid month '{month}'. Expected format YYYY-MM, e.g. 2023-01.")
    return month


def month_bounds(month: str) -> Tuple[pd.Timestamp, pd.Timestamp]:
    start = pd.Timestamp(f"{validate_month(month)}-01")
    return start, start + pd.offsets.MonthBegin(1)


def raw_file_for_month(month: str):
    return RAW_DATA_DIR / f"yellow_tripdata_{validate_month(month)}.parquet"
