"""Пересоздаёт semantic_layer.db с полным seed. Запуск: python seed_db.py

Полный список терминов — в seed_tuples.py (тот же источник, что и semantic_layer._SEED)."""

import json
import sqlite3
from pathlib import Path

from seed_tuples import SEED

DB_PATH = Path(__file__).parent / "semantic_layer.db"

SCHEMA = """
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


def seed() -> None:
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(SCHEMA)
    inserted = 0
    for term, kind, col, agg, flt, syns in SEED:
        try:
            conn.execute(
                "INSERT INTO terms(term,kind,column_expr,agg,filter_sql,synonyms_json,is_user_added) VALUES(?,?,?,?,?,?,0)",
                (term, kind, col, agg, flt, json.dumps(syns, ensure_ascii=False)),
            )
            inserted += 1
        except sqlite3.IntegrityError:
            pass
    conn.commit()
    total = conn.execute("SELECT COUNT(*) FROM terms").fetchone()[0]
    conn.close()
    print(f"Inserted {inserted} new terms. Total in DB: {total}")


if __name__ == "__main__":
    seed()
