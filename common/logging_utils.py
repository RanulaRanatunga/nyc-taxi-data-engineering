import json
import logging
import os
import sys
from datetime import datetime, timezone

from common.settings import LOG_DIR

_STANDARD_RECORD_ATTRS = set(
    vars(logging.LogRecord("", logging.INFO, "", 0, "", None, None)).keys()
) | {"message", "asctime", "taskName"}


class JsonFormatter(logging.Formatter):
    def __init__(self, component: str):
        super().__init__()
        self.component = component

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "component": self.component,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _STANDARD_RECORD_ATTRS and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


class ConsoleFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base = super().format(record)
        extras = {
            key: value
            for key, value in record.__dict__.items()
            if key not in _STANDARD_RECORD_ATTRS and not key.startswith("_")
        }
        if extras:
            base += " | " + " ".join(f"{key}={value}" for key, value in extras.items())
        return base


def setup_logging(component: str, level: int = logging.INFO, console: bool = True) -> None:
    root = logging.getLogger()
    if getattr(root, "_nyc_taxi_configured", False):
        return

    if console:
        console_handler = logging.StreamHandler(sys.stdout)
        if os.getenv("LOG_FORMAT", "json").lower() == "text":
            console_handler.setFormatter(
                ConsoleFormatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s", "%Y-%m-%d %H:%M:%S")
            )
        else:
            console_handler.setFormatter(JsonFormatter(component))
        root.addHandler(console_handler)

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(LOG_DIR / f"{component}.log", encoding="utf-8")
    file_handler.setFormatter(JsonFormatter(component))
    root.addHandler(file_handler)

    root.setLevel(level)
    for noisy in ("kafka", "urllib3", "httpx"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    root._nyc_taxi_configured = True
