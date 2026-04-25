"""Локальная SQLite для сохранённых отчётов + расписаний рассылки.

Таблицы:
  reports           — пользовательские отчёты (имя, NL-запрос, SQL, тип графика,
                      параметры расписания и канал доставки)
  tg_users          — chat_id телеграм-бота (нужен планировщику и боту)

Telegram-бот в корне (`telegram_bot.py`) и FastAPI используют эту же БД
для таблицы `tg_users`.
"""
from __future__ import annotations

import calendar
import re
import sqlite3
from pathlib import Path
from typing import Optional

DB_PATH = Path(__file__).parent / "reports.db"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS tg_users (
    telegram_user_id INTEGER PRIMARY KEY,
    chat_id          INTEGER NOT NULL,
    username         TEXT    DEFAULT '',
    full_name        TEXT    DEFAULT '',
    registered_at    TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    active           INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS reports (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    name             TEXT    NOT NULL,
    query            TEXT    NOT NULL,         -- NL-запрос
    sql              TEXT,                     -- закэшированный SQL (из последнего /ask)
    chart_type       TEXT    NOT NULL DEFAULT 'auto',
    -- расписание
    schedule_enabled INTEGER NOT NULL DEFAULT 1,
    freq             TEXT    NOT NULL DEFAULT 'weekly',   -- daily|weekdays|weekly|monthly
    day              TEXT,                     -- для weekly/monthly: 'Пн','Вт',... или '1'..'31'
    time             TEXT    NOT NULL DEFAULT '09:00',    -- HH:MM
    -- доставка
    channel          TEXT    NOT NULL DEFAULT 'telegram', -- telegram|email|both
    telegram_user_id INTEGER,
    email            TEXT    DEFAULT '',
    format           TEXT    NOT NULL DEFAULT 'pdf',      -- pdf|png|xlsx|csv
    -- мета
    created_at       TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at       TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_sent_at     TEXT
);

CREATE INDEX IF NOT EXISTS idx_reports_enabled ON reports(schedule_enabled);

CREATE TABLE IF NOT EXISTS app_settings (
    id                INTEGER PRIMARY KEY CHECK (id = 1),
    telegram_user_id  INTEGER,
    email             TEXT    NOT NULL DEFAULT '',
    channel           TEXT    NOT NULL DEFAULT 'both'
);
"""


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    return c


def init_db() -> None:
    with _conn() as c:
        c.executescript(_SCHEMA)
        c.execute("INSERT OR IGNORE INTO app_settings (id) VALUES (1)")
    _seed_reports_if_empty()


def _seed_reports_if_empty() -> None:
    """Заполняем стартовыми отчётами из макета, если БД пустая."""
    with _conn() as c:
        count = c.execute("SELECT COUNT(*) AS n FROM reports").fetchone()["n"]
        if count > 0:
            return
        c.executemany(
            """
            INSERT INTO reports (
                name, query, sql, chart_type,
                schedule_enabled, freq, day, time,
                channel, telegram_user_id, email, format
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "Отмены по городам · неделя",
                    "Покажи отмены по городам за прошлую неделю",
                    "SELECT city_id, COUNT(*) AS cancels FROM orders WHERE status_order = 'cancel' "
                    "AND order_timestamp >= NOW() - INTERVAL 7 DAY GROUP BY city_id ORDER BY cancels DESC LIMIT 50",
                    "bar",
                    1, "weekly", "Пн", "09:00",
                    "both", None, "ivan@company.ru", "pdf",
                ),
                (
                    "Выручка по месяцам · 2025",
                    "Покажи выручку по месяцам в 2025",
                    "SELECT MONTH(driverdone_timestamp) AS month, SUM(price_order_local) AS revenue FROM orders "
                    "WHERE status_order = 'done' AND YEAR(driverdone_timestamp) = 2025 "
                    "GROUP BY MONTH(driverdone_timestamp) ORDER BY month LIMIT 200",
                    "line",
                    0, "weekly", "Пн", "09:00",
                    "both", None, "ivan@company.ru", "pdf",
                ),
                (
                    "Доля отмен по каналам",
                    "Покажи долю отмен по каналам app, web, partner",
                    "SELECT source_name, COUNT(*) AS cancels FROM orders WHERE status_order = 'cancel' "
                    "GROUP BY source_name ORDER BY cancels DESC LIMIT 20",
                    "pie",
                    1, "monthly", "1", "09:00",
                    "both", None, "ivan@company.ru", "pdf",
                ),
                (
                    "Активные пользователи · 30д",
                    "Покажи активных пользователей по дням за последние 30 дней",
                    "SELECT DATE(order_timestamp) AS day, COUNT(DISTINCT user_id) AS active_users FROM orders "
                    "WHERE order_timestamp >= NOW() - INTERVAL 30 DAY GROUP BY DATE(order_timestamp) "
                    "ORDER BY day LIMIT 100",
                    "area",
                    1, "daily", None, "08:00",
                    "both", None, "ivan@company.ru", "pdf",
                ),
                (
                    "Топ-10 водителей",
                    "Покажи топ 10 водителей по количеству поездок",
                    "SELECT driver_id, COUNT(*) AS rides FROM orders WHERE status_order = 'done' "
                    "GROUP BY driver_id ORDER BY rides DESC LIMIT 10",
                    "table",
                    0, "weekly", "Пн", "09:00",
                    "both", None, "ivan@company.ru", "xlsx",
                ),
                (
                    "Эффективность рекламы",
                    "Покажи эффективность рекламных кампаний за 14 дней",
                    "SELECT source_name, COUNT(*) AS rides, AVG(price_order_local) AS avg_check FROM orders "
                    "WHERE order_timestamp >= NOW() - INTERVAL 14 DAY GROUP BY source_name "
                    "ORDER BY rides DESC LIMIT 30",
                    "bar",
                    1, "weekly", "Пт", "17:00",
                    "both", None, "ivan@company.ru", "pdf",
                ),
            ],
        )


_DAYS_CRON = {"Пн": 1, "Вт": 2, "Ср": 3, "Чт": 4, "Пт": 5, "Сб": 6, "Вс": 0}

_WEEKDAY_RU_LONG = {
    "понедельник": 1,
    "вторник": 2,
    "среда": 3,
    "четверг": 4,
    "пятница": 5,
    "суббота": 6,
    "воскресенье": 0,
}


def _weekday_to_dow(day: Optional[str]) -> int:
    if not day:
        return 1
    s = str(day).strip()
    key2 = s[:2].capitalize()
    if key2 in _DAYS_CRON:
        return _DAYS_CRON[key2]
    low = s.lower()
    if low in _WEEKDAY_RU_LONG:
        return _WEEKDAY_RU_LONG[low]
    return 1


def _month_day_to_dom(day: Optional[str]) -> str:
    if not day:
        return "1"
    m = re.search(r"(\d{1,2})", str(day))
    if not m:
        return "1"
    n = int(m.group(1))
    return str(max(1, min(31, n)))


def monthly_want_day_int(day: Optional[str]) -> int:
    return int(_month_day_to_dom(day))


def monthly_effective_dom(year: int, month: int, want: int) -> int:
    last = calendar.monthrange(year, month)[1]
    return min(max(1, want), last)


def to_cron(freq: str, day: Optional[str], time_hhmm: str) -> str:
    try:
        hh, mm = time_hhmm.split(":")
    except Exception:
        hh, mm = "9", "0"
    mm = int(mm)
    hh = int(hh)
    if freq == "daily":
        return f"{mm} {hh} * * *"
    if freq == "weekdays":
        return f"{mm} {hh} * * 1-5"
    if freq == "weekly":
        dow = _weekday_to_dow(day)
        return f"{mm} {hh} * * {dow}"
    if freq == "monthly":
        return f"{mm} {hh} * * *"
    return f"{mm} {hh} * * *"


_ALLOWED = {
    "name", "query", "sql", "chart_type",
    "schedule_enabled", "freq", "day", "time",
    "channel", "telegram_user_id", "email", "format",
}


def add_report(**kwargs) -> int:
    fields = {k: v for k, v in kwargs.items() if k in _ALLOWED}
    if "name" not in fields or "query" not in fields:
        raise ValueError("Нужны поля name и query")
    cols = ", ".join(fields)
    ph = ", ".join("?" for _ in fields)
    with _conn() as c:
        cur = c.execute(f"INSERT INTO reports ({cols}) VALUES ({ph})", tuple(fields.values()))
        return cur.lastrowid


def list_reports(enabled_only: bool = False) -> list[dict]:
    with _conn() as c:
        q = "SELECT * FROM reports"
        if enabled_only:
            q += " WHERE schedule_enabled = 1"
        q += " ORDER BY created_at DESC"
        rows = c.execute(q).fetchall()
    return [dict(r) for r in rows]


def get_report(report_id: int) -> Optional[dict]:
    with _conn() as c:
        row = c.execute("SELECT * FROM reports WHERE id = ?", (report_id,)).fetchone()
    return dict(row) if row else None


def update_report(report_id: int, **kwargs) -> None:
    fields = {k: v for k, v in kwargs.items() if k in _ALLOWED}
    if not fields:
        return
    fields["updated_at"] = None  # поставим CURRENT_TIMESTAMP через SQL
    sets = ", ".join(
        "updated_at = CURRENT_TIMESTAMP" if k == "updated_at" else f"{k} = ?"
        for k in fields
    )
    values = [v for k, v in fields.items() if k != "updated_at"]
    with _conn() as c:
        c.execute(f"UPDATE reports SET {sets} WHERE id = ?", (*values, report_id))


def delete_report(report_id: int) -> None:
    with _conn() as c:
        c.execute("DELETE FROM reports WHERE id = ?", (report_id,))


def mark_sent(report_id: int) -> None:
    with _conn() as c:
        c.execute(
            "UPDATE reports SET last_sent_at = CURRENT_TIMESTAMP WHERE id = ?",
            (report_id,),
        )


def save_chat_id(telegram_user_id: int, chat_id: int, username: str = "", full_name: str = "") -> None:
    with _conn() as c:
        c.execute(
            """
            INSERT INTO tg_users (telegram_user_id, chat_id, username, full_name, active)
            VALUES (?, ?, ?, ?, 1)
            ON CONFLICT(telegram_user_id) DO UPDATE SET
                chat_id = excluded.chat_id,
                username = excluded.username,
                full_name = excluded.full_name,
                active = 1
            """,
            (telegram_user_id, chat_id, username, full_name),
        )


def get_chat_id(telegram_user_id: int) -> Optional[int]:
    with _conn() as c:
        row = c.execute(
            "SELECT chat_id FROM tg_users WHERE telegram_user_id = ? AND active = 1",
            (telegram_user_id,),
        ).fetchone()
    return row["chat_id"] if row else None


def remove_chat_id(telegram_user_id: int) -> None:
    with _conn() as c:
        c.execute(
            "UPDATE tg_users SET active = 0 WHERE telegram_user_id = ?",
            (telegram_user_id,),
        )


def get_default_telegram_user_id() -> Optional[int]:
    """Последний активный пользователь, написавший боту /start (для отчётов без явного ID)."""
    with _conn() as c:
        row = c.execute(
            "SELECT telegram_user_id FROM tg_users WHERE active = 1 "
            "ORDER BY registered_at DESC LIMIT 1"
        ).fetchone()
    return int(row["telegram_user_id"]) if row else None


def last_dispatch_at_for_telegram_user(telegram_user_id: int) -> Optional[str]:
    """Максимальный last_sent_at по отчётам этого получателя (ISO-строка из SQLite)."""
    with _conn() as c:
        row = c.execute(
            "SELECT MAX(last_sent_at) AS m FROM reports "
            "WHERE telegram_user_id = ? AND last_sent_at IS NOT NULL",
            (telegram_user_id,),
        ).fetchone()
    if not row or row["m"] is None:
        return None
    return str(row["m"])


def get_app_delivery_settings() -> dict:
    """Глобальные настройки доставки из настроек (одна строка id=1)."""
    with _conn() as c:
        row = c.execute(
            "SELECT telegram_user_id, email, channel FROM app_settings WHERE id = 1"
        ).fetchone()
    if row is None:
        return {"telegram_user_id": None, "email": "", "channel": "both"}
    ch = (row["channel"] or "both").strip()
    if ch not in ("telegram", "email", "both"):
        ch = "both"
    return {
        "telegram_user_id": row["telegram_user_id"],
        "email": (row["email"] or "").strip(),
        "channel": ch,
    }


def save_app_delivery_settings(telegram_user_id: int, email: str, channel: str) -> None:
    if channel not in ("telegram", "email", "both"):
        channel = "both"
    with _conn() as c:
        c.execute(
            """
            UPDATE app_settings
            SET telegram_user_id = ?, email = ?, channel = ?
            WHERE id = 1
            """,
            (telegram_user_id, email or "", channel),
        )


def attach_orphan_reports_to_user(telegram_user_id: int) -> int:
    """Проставляет получателя всем отчётам без telegram_user_id (типичный одиночный аналитик)."""
    with _conn() as c:
        cur = c.execute(
            "UPDATE reports SET telegram_user_id = ?, updated_at = CURRENT_TIMESTAMP "
            "WHERE telegram_user_id IS NULL",
            (telegram_user_id,),
        )
        return cur.rowcount


init_db()
