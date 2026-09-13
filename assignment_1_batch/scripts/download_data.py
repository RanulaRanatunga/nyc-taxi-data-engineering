#!/usr/bin/env python3
import argparse
import logging
import shutil
import sys
import urllib.request
import zlib
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

BASE_DIR = Path(__file__).resolve().parents[2]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from assignment_1_batch.etl.config import (
    RAW_DATA_DIR,
    TLC_BASE_URL,
    ZONE_LOOKUP_FILE,
    ZONE_LOOKUP_URL,
    validate_month,
)

logger = logging.getLogger("etl.download")

USER_AGENT = {"User-Agent": "nyc-taxi-data-engineering/1.0"}


def _is_valid_parquet(path: Path) -> bool:
    try:
        pq.ParquetFile(path).metadata
        return True
    except Exception:
        return False


def _download(url: str, target: Path, timeout: int = 60) -> None:
    tmp_path = target.with_suffix(target.suffix + ".part")
    request = urllib.request.Request(url, headers=USER_AGENT)
    with urllib.request.urlopen(request, timeout=timeout) as response, open(tmp_path, "wb") as out_file:
        shutil.copyfileobj(response, out_file, length=1024 * 1024)
    tmp_path.replace(target)


def download_taxi_zone_lookup(target: Path = ZONE_LOOKUP_FILE) -> Path:
    if target.exists() and target.stat().st_size > 1000:
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    logger.info("downloading taxi zone lookup", extra={"url": ZONE_LOOKUP_URL})
    _download(ZONE_LOOKUP_URL, target)
    return target


def generate_synthetic_parquet(month: str, target_file: Path, num_records: int = 100_000) -> Path:
    rng = np.random.default_rng(zlib.crc32(month.encode()))
    start = pd.Timestamp(f"{month}-01")
    month_seconds = int(((start + pd.offsets.MonthBegin(1)) - start).total_seconds())

    pickups = start + pd.to_timedelta(np.sort(rng.integers(0, month_seconds, num_records)), unit="s")
    durations_min = np.clip(rng.lognormal(mean=2.3, sigma=0.6, size=num_records), 1, 120)
    distances = np.round(np.clip(durations_min * rng.uniform(0.1, 0.4, num_records), 0.1, 45.0), 2)
    fares = np.round(3.0 + distances * 3.5 + rng.uniform(0, 3, num_records), 2)
    payment_types = rng.choice([1, 2, 3, 4], p=[0.70, 0.28, 0.01, 0.01], size=num_records)
    tips = np.where(payment_types == 1, np.round(fares * rng.choice([0.15, 0.20, 0.25], size=num_records), 2), 0.0)
    extras = rng.choice([0.0, 0.5, 1.0, 2.5], p=[0.2, 0.3, 0.4, 0.1], size=num_records)
    tolls = np.where(distances > 10, rng.choice([0.0, 6.55], p=[0.6, 0.4], size=num_records), 0.0)
    congestion = rng.choice([0.0, 2.5], p=[0.1, 0.9], size=num_records)
    airport = np.where(distances > 15, rng.choice([0.0, 1.25], p=[0.7, 0.3], size=num_records), 0.0)

    df = pd.DataFrame({
        "VendorID": rng.choice([1, 2], size=num_records),
        "tpep_pickup_datetime": pickups,
        "tpep_dropoff_datetime": pickups + pd.to_timedelta(durations_min, unit="m"),
        "passenger_count": rng.choice([1, 2, 3, 4, 5, 6], p=[0.65, 0.18, 0.06, 0.04, 0.04, 0.03], size=num_records),
        "trip_distance": distances,
        "RatecodeID": rng.choice([1, 2, 3, 4, 5], p=[0.92, 0.05, 0.01, 0.01, 0.01], size=num_records),
        "store_and_fwd_flag": rng.choice(["N", "Y"], p=[0.99, 0.01], size=num_records),
        "PULocationID": rng.integers(1, 264, num_records),
        "DOLocationID": rng.integers(1, 264, num_records),
        "payment_type": payment_types,
        "fare_amount": fares,
        "extra": extras,
        "mta_tax": 0.5,
        "tip_amount": tips,
        "tolls_amount": tolls,
        "improvement_surcharge": 1.0,
        "total_amount": np.round(fares + extras + 0.5 + tips + tolls + 1.0 + congestion + airport, 2),
        "congestion_surcharge": congestion,
        "airport_fee": airport,
    })
    target_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(target_file, index=False)
    logger.warning("generated SYNTHETIC data", extra={"file": target_file.name, "rows": num_records})
    return target_file


def download_month_data(
    month: str,
    data_dir: Path = RAW_DATA_DIR,
    force_download: bool = False,
    allow_synthetic_fallback: bool = False,
    synthetic_rows: int = 100_000,
) -> Path:
    validate_month(month)
    data_dir.mkdir(parents=True, exist_ok=True)
    target = data_dir / f"yellow_tripdata_{month}.parquet"

    if target.exists() and not force_download:
        if _is_valid_parquet(target):
            logger.info("file already present", extra={"file": target.name})
            return target
        logger.warning("existing file is corrupt, re-downloading", extra={"file": target.name})

    url = f"{TLC_BASE_URL}/{target.name}"
    try:
        logger.info("downloading", extra={"url": url})
        _download(url, target)
        if not _is_valid_parquet(target):
            raise ValueError(f"Downloaded file {target.name} is not a valid parquet file")
        logger.info("download finished", extra={"file": target.name,
                                                "size_mb": round(target.stat().st_size / 1024 / 1024, 2)})
        return target
    except Exception as exc:
        target.unlink(missing_ok=True)
        if not allow_synthetic_fallback:
            raise RuntimeError(f"Could not download {url}: {exc}") from exc
        logger.warning("download failed, using synthetic fallback", extra={"month": month, "error": str(exc)})
        return generate_synthetic_parquet(month, target, num_records=synthetic_rows)


def main() -> None:
    from common.logging_utils import setup_logging

    parser = argparse.ArgumentParser(description="Download NYC yellow taxi trip data")
    parser.add_argument("--months", nargs="+", default=["2023-01", "2023-02"], help="Months (YYYY-MM)")
    parser.add_argument("--force", action="store_true", help="Re-download even if the file exists")
    parser.add_argument("--allow-synthetic-fallback", action="store_true",
                        help="Generate synthetic data when the download fails (offline demo)")
    parser.add_argument("--synthetic-rows", type=int, default=100_000)
    args = parser.parse_args()

    setup_logging("download")
    download_taxi_zone_lookup()
    for month in args.months:
        download_month_data(month, RAW_DATA_DIR, args.force, args.allow_synthetic_fallback, args.synthetic_rows)


if __name__ == "__main__":
    main()
