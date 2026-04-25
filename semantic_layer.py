"""Семантический слой: SQLite + кэш, валидация колонок по INFORMATION_SCHEMA."""
from __future__ import annotations

import json
import sqlite3
import threading
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

from sqlalchemy import text

from config import engine_admin, DB_NAME

DB_PATH = Path(__file__).parent / "semantic_layer.db"

KIND_METRIC    = "metric"
KIND_DIMENSION = "dimension"
KIND_FILTER    = "filter"
KIND_PERIOD    = "period"
KIND_ID        = "id"
KIND_TIME      = "time"
KIND_TRIP      = "trip"
KIND_STATUS    = "status"
VALID_KINDS = {KIND_METRIC, KIND_DIMENSION, KIND_FILTER, KIND_PERIOD,
               KIND_ID, KIND_TIME, KIND_TRIP, KIND_STATUS}


@dataclass
class Term:
    term: str
    kind: str
    column_expr: str
    agg: Optional[str] = None
    filter_sql: Optional[str] = None
    synonyms: list[str] = None  # type: ignore
    is_user_added: bool = False
    id: Optional[int] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    def to_dict(self) -> dict:
        d = asdict(self)
        d["synonyms"] = self.synonyms or []
        return d


_SCHEMA = """
CREATE TABLE IF NOT EXISTS terms (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    term TEXT NOT NULL UNIQUE COLLATE NOCASE,
    kind TEXT NOT NULL,
    column_expr TEXT NOT NULL,
    agg TEXT,
    filter_sql TEXT,
    synonyms_json TEXT NOT NULL DEFAULT '[]',
    is_user_added INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS term_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    term_id INTEGER,
    term TEXT NOT NULL,
    action TEXT NOT NULL,
    payload_json TEXT,
    actor TEXT DEFAULT 'system',
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_terms_kind ON terms(kind);
"""


_SEED: list[Term] = [
    Term("город (id)",             KIND_ID,        "city_id",                      None, None,
         ["город", "city", "регион"]),
    Term("часовой пояс",           KIND_ID,        "offset_hours",                 None, None,
         ["UTC", "timezone", "пояс"]),
    Term("заказ",                  KIND_ID,        "order_id",                     None, None,
         ["заказ", "order", "поездка"]),
    Term("тендер",                 KIND_ID,        "tender_id",                    None, None,
         ["тендер", "подбор", "аукцион"]),
    Term("пользователь (клиент)",  KIND_ID,        "user_id",                      None, None,
         ["клиент", "user", "пассажир"]),
    Term("водитель",               KIND_ID,        "driver_id",                    None, None,
         ["водители", "driver", "drivers", "топ водителей", "исполнитель"]),

    Term("статус заказа",          KIND_STATUS,    "status_order",                 None, None,
         ["статус", "отмена", "выполнен", "cancelled", "completed", "by status"]),
    Term("статус тендера",         KIND_STATUS,    "status_tender",                None, None,
         ["подбор", "тендер", "auction"]),

    Term("создание заказа",        KIND_TIME,      "order_timestamp",              None, None,
         ["создан", "заказан", "created_at"]),
    Term("начало тендера",         KIND_TIME,      "tender_timestamp",             None, None,
         ["подбор", "tender"]),
    Term("принятие водителем",     KIND_TIME,      "driveraccept_timestamp",       None, None,
         ["accept", "назначен", "взял заказ"]),
    Term("прибытие водителя",      KIND_TIME,      "driverarrived_timestamp",      None, None,
         ["подача", "arrived", "приехал"]),
    Term("начало поездки",         KIND_TIME,      "driverstarttheride_timestamp", None, None,
         ["start", "поехали", "старт"]),
    Term("завершение поездки",     KIND_TIME,      "driverdone_timestamp",         None, None,
         ["done", "completed", "финиш", "закончилась"]),
    Term("отмена клиентом",        KIND_TIME,      "clientcancel_timestamp",       None, None,
         ["клиент отменил", "client cancel"]),
    Term("отмена водителем",       KIND_TIME,      "drivercancel_timestamp",       None, None,
         ["водитель отменил", "driver cancel"]),
    Term("последнее изменение",    KIND_TIME,      "order_modified_local",         None, None,
         ["updated", "изменён"]),
    Term("отмена до принятия",     KIND_TIME,      "cancel_before_accept_local",   None, None,
         ["pre-accept cancel", "быстрая отмена"]),

    Term("расстояние",             KIND_TRIP,      "distance_in_meters",           None, None,
         ["дистанция", "distance", "км", "метры"]),
    Term("длительность",           KIND_TRIP,      "duration_in_seconds",          None, None,
         ["время поездки", "duration", "секунды", "минуты"]),
    Term("стоимость заказа",       KIND_TRIP,      "price_order_local",            None, None,
         ["цена", "price", "итог", "стоимость"]),
    Term("стоимость тендера",      KIND_TRIP,      "price_tender_local",           None, None,
         ["тендерная цена", "tender price"]),
    Term("стартовая стоимость",    KIND_TRIP,      "price_start_local",            None, None,
         ["начальная цена", "start price"]),

    Term("отмены",                 KIND_METRIC,    "status_order",  "COUNT(*)",
         "status_order = 'cancel'",
         ["отмена", "отменен", "canceled", "cancelled", "cancel"]),
    Term("поездки",                KIND_METRIC,    "status_order",  "COUNT(*)",
         "status_order = 'done'",
         ["поездка", "заказы", "ride", "rides", "order", "orders", "trips"]),
    Term("выручка",                KIND_METRIC,    "price_order_local", "SUM(price_order_local)",
         "price_order_local IS NOT NULL AND status_order = 'done'",
         ["доход", "revenue", "оборот", "sales", "gmv"]),
    Term("средняя цена",           KIND_METRIC,    "price_order_local", "AVG(price_order_local)",
         "price_order_local IS NOT NULL",
         ["цена", "price", "средний чек", "стоимость", "avg price"]),
    Term("город",                  KIND_DIMENSION, "city_id",           None, None,
         ["города", "city", "cities", "по городам"]),
    Term("дата",                   KIND_DIMENSION, "DATE(order_timestamp)", None, None,
         ["день", "дни", "date", "dates", "по датам", "по дням"]),
    Term("месяц",                  KIND_DIMENSION, "MONTH(order_timestamp)", None, None,
         ["месяц", "month", "по месяцам"]),
    Term("статус",                 KIND_DIMENSION, "status_order",      None, None,
         ["статусы", "status", "by status"]),
]


class DynamicSemanticLayer:
    def __init__(self, db_path: Path = DB_PATH) -> None:
        self.db_path = db_path
        self._lock = threading.RLock()
        self._cache_by_term: dict[str, Term] = {}
        self._cache_by_synonym: dict[str, Term] = {}
        self._schema_columns: set[str] = set()
        self._init_db()
        self._load_schema_columns()
        self._seed_if_empty()
        self._rebuild_cache()

    def _conn(self) -> sqlite3.Connection:
        c = sqlite3.connect(self.db_path)
        c.row_factory = sqlite3.Row
        return c

    def _init_db(self) -> None:
        with self._conn() as c:
            c.executescript(_SCHEMA)

    def _load_schema_columns(self) -> None:
        try:
            with engine_admin.connect() as conn:
                rows = conn.execute(text("""
                    SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = :db AND TABLE_NAME = 'orders'
                """), {"db": DB_NAME}).fetchall()
            self._schema_columns = {r[0] for r in rows}
        except Exception:
            self._schema_columns = set()

    def _seed_if_empty(self) -> None:
        with self._conn() as c:
            n = c.execute("SELECT COUNT(*) FROM terms").fetchone()[0]
            if n > 0:
                return
            for t in _SEED:
                c.execute(
                    "INSERT INTO terms(term,kind,column_expr,agg,filter_sql,synonyms_json,is_user_added) "
                    "VALUES(?,?,?,?,?,?,0)",
                    (t.term, t.kind, t.column_expr, t.agg, t.filter_sql,
                     json.dumps(t.synonyms or [], ensure_ascii=False)),
                )

    def _rebuild_cache(self) -> None:
        with self._lock, self._conn() as c:
            rows = c.execute("SELECT * FROM terms").fetchall()
            by_term: dict[str, Term] = {}
            by_syn: dict[str, Term] = {}
            for r in rows:
                t = self._row_to_term(r)
                by_term[t.term.lower()] = t
                for s in (t.synonyms or []):
                    by_syn[s.lower()] = t
            self._cache_by_term = by_term
            self._cache_by_synonym = by_syn

    @staticmethod
    def _row_to_term(r: sqlite3.Row) -> Term:
        return Term(
            id=r["id"],
            term=r["term"],
            kind=r["kind"],
            column_expr=r["column_expr"],
            agg=r["agg"],
            filter_sql=r["filter_sql"],
            synonyms=json.loads(r["synonyms_json"] or "[]"),
            is_user_added=bool(r["is_user_added"]),
            created_at=r["created_at"],
            updated_at=r["updated_at"],
        )

    def validate_column_expr(self, expr: str) -> tuple[bool, str]:
        """Ищем имена, которые похожи на колонки, и проверяем по INFO_SCHEMA.
        Не полный парсинг SQL — этого достаточно для нашего узкого формата
        `col` | `FUNC(col)` | `FUNC(col_a, col_b)`.
        """
        if not self._schema_columns:
            return True, "schema unknown, skipping strict check"
        import re
        tokens = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", expr)
        sql_funcs = {"DATE", "COUNT", "SUM", "AVG", "MIN", "MAX", "DAY", "MONTH",
                     "YEAR", "HOUR", "CAST", "IFNULL", "COALESCE"}
        unknown = [t for t in tokens if t.upper() not in sql_funcs
                   and t not in self._schema_columns]
        if unknown:
            return False, f"Неизвестные колонки: {', '.join(unknown)}"
        return True, "ok"

    def get(self, text_in: str) -> Optional[Term]:
        """Поиск по основному термину ИЛИ синониму (case-insensitive)."""
        key = text_in.strip().lower()
        with self._lock:
            return self._cache_by_term.get(key) or self._cache_by_synonym.get(key)

    def find_in_query(self, query: str) -> list[Term]:
        """Все термины, упомянутые в запросе. Сверяем токены запроса со
        стеммированными префиксами ключей — чтобы ловить падежи ('выручку',
        'поездку', 'отменено', 'водителей').
        """
        import re
        q_tokens = re.findall(r"[а-яёa-z0-9]+", query.lower())
        found: dict[int, Term] = {}
        with self._lock:
            items = list({**self._cache_by_term, **self._cache_by_synonym}.items())

        for key, term in items:
            if self._key_matches_tokens(key, q_tokens):
                found[term.id or id(term)] = term
        return list(found.values())

    @staticmethod
    def _key_matches_tokens(key: str, tokens: list[str]) -> bool:
        """Многословный ключ требует, чтобы все его слова нашлись (стем-префикс);
        односложный — совпадение по общему префиксу длины `min(len, len_tok)-2`,
        но не короче 3 символов.
        """
        parts = key.split()
        for p in parts:
            stem_len = max(3, len(p) - 2)
            stem = p[:stem_len]
            if not any(tok.startswith(stem) and tok[:stem_len] == stem for tok in tokens):
                return False
        return True

    def list_all(self, kind: Optional[str] = None) -> list[dict]:
        with self._lock:
            terms = list(self._cache_by_term.values())
        if kind:
            terms = [t for t in terms if t.kind == kind]
        return [t.to_dict() for t in terms]

    def add(self, term: str, kind: str, column_expr: str,
            agg: Optional[str] = None, filter_sql: Optional[str] = None,
            synonyms: Optional[list[str]] = None, actor: str = "user") -> Term:
        if kind not in VALID_KINDS:
            raise ValueError(f"kind должен быть одним из {VALID_KINDS}")
        ok, msg = self.validate_column_expr(column_expr)
        if not ok:
            raise ValueError(f"Валидация column_expr не прошла: {msg}")

        synonyms = [s.strip() for s in (synonyms or []) if s.strip()]
        with self._lock, self._conn() as c:
            cur = c.execute(
                "INSERT INTO terms(term,kind,column_expr,agg,filter_sql,synonyms_json,is_user_added) "
                "VALUES(?,?,?,?,?,?,1)",
                (term, kind, column_expr, agg, filter_sql,
                 json.dumps(synonyms, ensure_ascii=False)),
            )
            term_id = cur.lastrowid
            c.execute(
                "INSERT INTO term_history(term_id,term,action,payload_json,actor) VALUES(?,?,?,?,?)",
                (term_id, term, "add",
                 json.dumps({"kind": kind, "column_expr": column_expr, "agg": agg,
                             "filter_sql": filter_sql, "synonyms": synonyms}, ensure_ascii=False),
                 actor),
            )
        self._rebuild_cache()
        return self.get(term)  # type: ignore[return-value]

    def add_synonym(self, term: str, synonym: str, actor: str = "auto") -> bool:
        """1-click-подтверждённое добавление синонима к существующему термину."""
        t = self.get(term)
        if not t:
            return False
        syns = [s.lower() for s in (t.synonyms or [])]
        if synonym.lower() in syns or synonym.lower() == t.term.lower():
            return False
        new_syns = (t.synonyms or []) + [synonym]
        with self._lock, self._conn() as c:
            c.execute(
                "UPDATE terms SET synonyms_json=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (json.dumps(new_syns, ensure_ascii=False), t.id),
            )
            c.execute(
                "INSERT INTO term_history(term_id,term,action,payload_json,actor) VALUES(?,?,?,?,?)",
                (t.id, t.term, "synonym_add",
                 json.dumps({"synonym": synonym}, ensure_ascii=False), actor),
            )
        self._rebuild_cache()
        return True

    def remove_synonym(self, term: str, synonym: str, actor: str = "user") -> bool:
        """Удаляет синоним у термина (case-insensitive)."""
        t = self.get(term)
        if not t:
            return False
        original = t.synonyms or []
        kept = [s for s in original if s.lower() != synonym.lower()]
        if len(kept) == len(original):
            return False
        with self._lock, self._conn() as c:
            c.execute(
                "UPDATE terms SET synonyms_json=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (json.dumps(kept, ensure_ascii=False), t.id),
            )
            c.execute(
                "INSERT INTO term_history(term_id,term,action,payload_json,actor) VALUES(?,?,?,?,?)",
                (t.id, t.term, "synonym_delete",
                 json.dumps({"synonym": synonym}, ensure_ascii=False), actor),
            )
        self._rebuild_cache()
        return True

    def delete(self, term: str, actor: str = "user") -> bool:
        t = self.get(term)
        if not t:
            return False
        with self._lock, self._conn() as c:
            c.execute("DELETE FROM terms WHERE id=?", (t.id,))
            c.execute(
                "INSERT INTO term_history(term_id,term,action,payload_json,actor) VALUES(?,?,?,?,?)",
                (t.id, t.term, "delete", None, actor),
            )
        self._rebuild_cache()
        return True

    def history(self, limit: int = 100) -> list[dict]:
        with self._conn() as c:
            rows = c.execute(
                "SELECT * FROM term_history ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(r) for r in rows]


semantic = DynamicSemanticLayer()
