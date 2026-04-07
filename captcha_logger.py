"""Добавляет строки в JSONL для последующего разбора капч (обучение / отладка)."""
import json
import time
from datetime import datetime, timezone
from pathlib import Path


def log_captcha_event(project_dir: Path, log_filename: str | None, payload: dict) -> None:
    if not log_filename or not str(log_filename).strip():
        return
    path = project_dir / str(log_filename).strip()
    row = {
        "ts": time.time(),
        "ts_iso": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        **payload,
    }
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")
