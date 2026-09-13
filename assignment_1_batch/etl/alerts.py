import json
import logging
from datetime import datetime, timezone

import requests

from common.settings import LOG_DIR, env_str

logger = logging.getLogger("etl.alerts")


def send_alert(title: str, details: dict) -> None:
    alert = {"timestamp": datetime.now(timezone.utc).isoformat(), "title": title, **details}
    logger.critical(title, extra={"alert": True, **details})

    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        with open(LOG_DIR / "alerts.jsonl", "a", encoding="utf-8") as handle:
            handle.write(json.dumps(alert, default=str) + "\n")
    except OSError as exc:
        logger.error("could not write alert file", extra={"error": str(exc)})

    webhook_url = env_str("ALERT_WEBHOOK_URL", "")
    if webhook_url:
        text = f":rotating_light: {title}\n" + "\n".join(f"*{key}*: {value}" for key, value in details.items())
        try:
            requests.post(webhook_url, json={"text": text}, timeout=5).raise_for_status()
        except requests.RequestException as exc:
            logger.error("alert webhook delivery failed", extra={"error": str(exc)})


def task_failure_hook(task, task_run, state) -> None:
    send_alert(
        f"Prefect task failed: {task_run.name}",
        {"task": task.name, "task_run_id": str(task_run.id), "state": state.name, "state_message": state.message},
    )


def flow_failure_hook(flow, flow_run, state) -> None:
    send_alert(
        f"Prefect flow failed: {flow_run.name}",
        {
            "flow": flow.name,
            "flow_run_id": str(flow_run.id),
            "parameters": flow_run.parameters,
            "state": state.name,
            "state_message": state.message,
        },
    )
