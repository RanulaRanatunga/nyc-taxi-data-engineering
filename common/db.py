import logging
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, List, Optional, Sequence, Tuple

import pandas as pd

from common.settings import DATA_DIR, env_bool, env_int, env_str, resolve_path

logger = logging.getLogger("common.db")

POSTGRES = "postgres"
DUCKDB = "duckdb"


@dataclass
class DatabaseConfig:
    engine_type: str = POSTGRES
    pg_host: str = "localhost"
    pg_port: int = 5432
    pg_db: str = "nyc_taxi"
    pg_user: str = "postgres"
    pg_password: str = "postgres"
    duckdb_path: Path = DATA_DIR / "nyc_taxi.duckdb"
    allow_duckdb_fallback: bool = True

    @classmethod
    def from_env(
        cls,
        prefix: str = "",
        default_db: str = "nyc_taxi",
        default_port: int = 5432,
        default_duckdb_path: str = "data/nyc_taxi.duckdb",
    ) -> "DatabaseConfig":
        return cls(
            engine_type=env_str(f"{prefix}DB_ENGINE", POSTGRES).lower(),
            pg_host=env_str(f"{prefix}POSTGRES_HOST", "localhost"),
            pg_port=env_int(f"{prefix}POSTGRES_PORT", default_port),
            pg_db=env_str(f"{prefix}POSTGRES_DB", default_db),
            pg_user=env_str(f"{prefix}POSTGRES_USER", "postgres"),
            pg_password=env_str(f"{prefix}POSTGRES_PASSWORD", "postgres"),
            duckdb_path=resolve_path(env_str(f"{prefix}DUCKDB_PATH", default_duckdb_path)),
            allow_duckdb_fallback=env_bool(f"{prefix}DB_ALLOW_DUCKDB_FALLBACK", True),
        )


def _connect_duckdb(cfg: DatabaseConfig, read_only: bool):
    import duckdb

    cfg.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    if read_only and not cfg.duckdb_path.exists():
        read_only = False
    return duckdb.connect(str(cfg.duckdb_path), read_only=read_only)


def get_db_connection(cfg: Optional[DatabaseConfig] = None, read_only: bool = False) -> Tuple[Any, str]:
    cfg = cfg or DatabaseConfig.from_env()

    if cfg.engine_type == DUCKDB:
        return _connect_duckdb(cfg, read_only), DUCKDB
    if cfg.engine_type != POSTGRES:
        raise ValueError(f"Unsupported DB engine '{cfg.engine_type}'. Use 'postgres' or 'duckdb'.")

    import psycopg2

    try:
        conn = psycopg2.connect(
            host=cfg.pg_host,
            port=cfg.pg_port,
            dbname=cfg.pg_db,
            user=cfg.pg_user,
            password=cfg.pg_password,
            connect_timeout=5,
        )
        return conn, POSTGRES
    except psycopg2.OperationalError as exc:
        if not cfg.allow_duckdb_fallback:
            raise
        logger.warning(
            "PostgreSQL unreachable, falling back to DuckDB",
            extra={"pg_host": cfg.pg_host, "pg_port": cfg.pg_port, "duckdb_path": str(cfg.duckdb_path),
                   "error": str(exc).strip()},
        )
        return _connect_duckdb(cfg, read_only), DUCKDB


def split_sql_statements(sql: str) -> List[str]:
    without_comments = "\n".join(line for line in sql.splitlines() if not line.strip().startswith("--"))
    return [statement.strip() for statement in without_comments.split(";") if statement.strip()]


def placeholder(engine_type: str) -> str:
    return "%s" if engine_type == POSTGRES else "?"


def fetch_df(conn, sql: str, params: Optional[Sequence[Any]] = None) -> pd.DataFrame:
    cursor = conn.cursor()
    try:
        if params is None:
            cursor.execute(sql)
        else:
            cursor.execute(sql, params)
        columns = [col[0] for col in cursor.description]
        df = pd.DataFrame(cursor.fetchall(), columns=columns)
    finally:
        cursor.close()

    for column in df.columns:
        non_null = df[column].dropna()
        if not non_null.empty and isinstance(non_null.iloc[0], Decimal):
            df[column] = df[column].astype(float)
    return df
