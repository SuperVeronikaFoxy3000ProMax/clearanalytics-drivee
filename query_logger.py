"""Логирование NL-запросов: JSONL + человекочитаемый лог.

JSONL-файл — для дашборда аналитика / будущего UI «история».
Текстовый лог — для быстрого просмотра `tail -f`.
"""
from __future__ import annotations

import json
import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

LOGS_DIR = Path(__file__).parent / "logs"
LOGS_DIR.mkdir(exist_ok=True)

JSONL_PATH = LOGS_DIR / "query_logs.jsonl"
TEXT_PATH = LOGS_DIR / "query_logs.log"

_text_logger = logging.getLogger("drivee.nl_query")
if not _text_logger.handlers:
    _text_logger.setLevel(logging.INFO)
    h = logging.FileHandler(TEXT_PATH, encoding="utf-8")
    h.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    _text_logger.addHandler(h)


@dataclass
class QueryLogEntry:
    ts: str = field(default_factory=lambda: datetime.now().isoformat(timespec="seconds"))
    query: str = ""
    route_type: str = "unknown"     # rule | llm | template | unknown
    sql: Optional[str] = None
    exec_ms: Optional[int] = None
    rows: Optional[int] = None
    status: str = "ok"              # ok | error | blocked | ambiguous | not_recognized
    confidence: Optional[float] = None
    error: Optional[str] = None
    extras: dict = field(default_factory=dict)


def write(entry: QueryLogEntry) -> None:
    data = asdict(entry)
    with JSONL_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(data, ensure_ascii=False) + "\n")
    level = logging.ERROR if entry.status == "error" else logging.INFO
    _text_logger.log(
        level,
        "[%s] route=%s status=%s rows=%s ms=%s conf=%s | %s",
        entry.ts, entry.route_type, entry.status,
        entry.rows, entry.exec_ms, entry.confidence, entry.query,
    )


@contextmanager
def log_query(query: str, route_type: str = "unknown"):
    """Context-manager: заполняет exec_ms автоматически, ловит исключения.

    Использование:
        with log_query("покажи выручку", route_type="rule") as entry:
            entry.sql = sql
            entry.rows = len(rows)
            entry.confidence = 0.9
    """
    entry = QueryLogEntry(query=query, route_type=route_type)
    t0 = time.monotonic()
    try:
        yield entry
    except Exception as e:
        entry.status = "error"
        entry.error = f"{type(e).__name__}: {e}"
        raise
    finally:
        entry.exec_ms = int((time.monotonic() - t0) * 1000)
        write(entry)


def tail(n: int = 50) -> list[dict]:
    """Последние N записей — для /logs эндпоинта."""
    if not JSONL_PATH.exists():
        return []
    with JSONL_PATH.open(encoding="utf-8") as f:
        lines = f.readlines()[-n:]
    out = []
    for ln in lines:
        try:
            out.append(json.loads(ln))
        except json.JSONDecodeError:
            continue
    return out
