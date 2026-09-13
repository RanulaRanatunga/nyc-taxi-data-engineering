import logging
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger("etl.extract")

REQUIRED_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "total_amount",
]

OPTIONAL_COLUMNS = [
    "store_and_fwd_flag",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "congestion_surcharge",
    "airport_fee",
]

_CANONICAL_NAMES = {name.lower(): name for name in REQUIRED_COLUMNS + OPTIONAL_COLUMNS}


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    rename = {col: _CANONICAL_NAMES[col.lower()] for col in df.columns if col.lower() in _CANONICAL_NAMES}
    return df.rename(columns=rename)


def validate_schema(df: pd.DataFrame, file_path: Path) -> bool:
    present = {col.lower() for col in df.columns}
    missing = [col for col in REQUIRED_COLUMNS if col.lower() not in present]
    if missing:
        raise ValueError(f"Schema validation failed for {file_path.name}. Missing required columns: {missing}")
    return True


def extract_parquet(file_path: Path, sample_limit: Optional[int] = None) -> Tuple[pd.DataFrame, dict]:
    if not file_path.exists():
        raise FileNotFoundError(f"Parquet source file not found at: {file_path}")

    parquet_file = pq.ParquetFile(file_path)
    total_rows = parquet_file.metadata.num_rows
    file_size_mb = round(file_path.stat().st_size / (1024 * 1024), 2)
    logger.info(
        "extract started",
        extra={"source_file": file_path.name, "file_size_mb": file_size_mb, "total_file_rows": total_rows},
    )

    if sample_limit and sample_limit < total_rows:
        batches, rows_read = [], 0
        for batch in parquet_file.iter_batches(batch_size=min(sample_limit, 100_000)):
            batches.append(batch)
            rows_read += batch.num_rows
            if rows_read >= sample_limit:
                break
        table = pa.Table.from_batches(batches).slice(0, sample_limit)
    else:
        table = parquet_file.read()

    df = normalize_columns(table.to_pandas())
    validate_schema(df, file_path)

    metadata = {
        "source_file": file_path.name,
        "file_size_mb": file_size_mb,
        "total_file_rows": total_rows,
        "rows_extracted": len(df),
        "columns_count": len(df.columns),
    }
    logger.info("extract finished", extra=metadata)
    return df, metadata
