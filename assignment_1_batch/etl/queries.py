from pathlib import Path
from typing import Dict

from assignment_1_batch.etl.config import SQL_DIR

QUERIES_DIR = SQL_DIR / "analytical_queries"


def load_named_queries(directory: Path = QUERIES_DIR) -> Dict[str, str]:
    return {
        path.stem: path.read_text(encoding="utf-8").strip().rstrip(";").strip()
        for path in sorted(directory.glob("*.sql"))
    }
