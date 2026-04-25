"""Проверка SQL: sqlglot AST + sqlparse, whitelist таблиц, принудительный LIMIT."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

import sqlparse
import sqlglot
from sqlglot import expressions as exp

from config import FORCED_LIMIT, DATA_TABLES

ALLOWED_TABLES = set(DATA_TABLES)

FORBIDDEN_EXPR_TYPES = (
    exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.Create,
    exp.Alter, exp.AlterColumn, exp.TruncateTable,
    exp.Grant,
    exp.Command,
)


class GuardrailError(ValueError):
    """Запрос не прошёл проверку безопасности."""
    def __init__(self, reason: str, code: str = "guardrail_blocked"):
        super().__init__(reason)
        self.reason = reason
        self.code = code


@dataclass
class GuardrailReport:
    """Результат проверки: безопасный SQL + метаданные сложности для диспетчера."""
    sql: str
    tables: list[str]
    has_join: bool
    has_subquery: bool
    has_cte: bool
    has_aggregate: bool
    had_limit: bool
    complexity: int


def validate(sql: str) -> GuardrailReport:
    if not sql or not sql.strip():
        raise GuardrailError("Пустой SQL")

    statements = [s for s in sqlparse.parse(sql) if s.tokens and str(s).strip()]
    if len(statements) != 1:
        raise GuardrailError(
            f"Разрешено только одно SQL-выражение, получено: {len(statements)}"
        )

    try:
        tree = sqlglot.parse_one(sql, read="mysql")
    except Exception as e:
        raise GuardrailError(f"SQL не распарсился: {e}", code="parse_error")

    root_select = tree if isinstance(tree, exp.Select) else tree.find(exp.Select)
    if not isinstance(tree, (exp.Select, exp.Subquery, exp.With)) or root_select is None:
        raise GuardrailError(f"Ожидался SELECT, получен {type(tree).__name__}")

    for node in tree.walk():
        n = node[0] if isinstance(node, tuple) else node
        if isinstance(n, FORBIDDEN_EXPR_TYPES):
            raise GuardrailError(
                f"Запрещённая операция: {type(n).__name__}",
                code="dml_ddl_blocked",
            )

    table_nodes = list(tree.find_all(exp.Table))
    tables = []
    for t in table_nodes:
        name = t.name.lower() if t.name else ""
        tables.append(name)
        if name not in ALLOWED_TABLES:
            raise GuardrailError(
                f"Обращение к таблице '{name}' не разрешено. Доступно: {sorted(ALLOWED_TABLES)}",
                code="table_not_allowed",
            )
    if not tables:
        raise GuardrailError("Нет FROM-таблицы в запросе", code="no_from")

    has_join = any(True for _ in tree.find_all(exp.Join))
    has_subquery = any(sq for sq in tree.find_all(exp.Subquery) if sq is not tree)
    has_cte = bool(tree.find(exp.With))
    has_aggregate = any(True for _ in tree.find_all(
        exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max
    ))

    complexity = _score_complexity(has_join, has_subquery, has_cte, len(tables))

    had_limit = root_select.args.get("limit") is not None
    if not had_limit:
        root_select.set("limit", exp.Limit(
            expression=exp.Literal.number(FORCED_LIMIT)
        ))

    normalized = tree.sql(dialect="mysql")

    if normalized.strip().rstrip(";").count(";") > 0:
        raise GuardrailError("Несколько выражений через ';' запрещены")

    return GuardrailReport(
        sql=normalized,
        tables=list(dict.fromkeys(tables)),
        has_join=has_join,
        has_subquery=has_subquery,
        has_cte=has_cte,
        has_aggregate=has_aggregate,
        had_limit=had_limit,
        complexity=complexity,
    )


def _score_complexity(join: bool, subquery: bool, cte: bool, n_tables: int) -> int:
    score = 0
    if n_tables > 1:
        score += 1
    if join:
        score += 1
    if subquery:
        score += 1
    if cte:
        score += 1
    return min(2, score)


_SELECT_STAR_RE = re.compile(r"select\s+\*", re.IGNORECASE)


def performance_warnings(sql: str) -> list[str]:
    warns: list[str] = []
    low = sql.lower()
    if _SELECT_STAR_RE.search(low):
        warns.append("Использован SELECT * — лучше перечислить нужные колонки.")
    if " where " not in low and "count(" not in low:
        low_norm = " ".join(low.split())
        for t in DATA_TABLES:
            if re.search(rf"\bfrom\s+`?{t}`?\b", low_norm):
                warns.append(f"Запрос без WHERE по таблице {t} — возможен полный скан.")
                break
    if "order by" in low and "limit" not in low:
        warns.append("ORDER BY без LIMIT — сортировка всей таблицы.")
    return warns
