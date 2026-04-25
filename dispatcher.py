"""NL→SQL dispatcher."""
from __future__ import annotations

import re
from dataclasses import dataclass, field, asdict
from typing import Optional

from sqlalchemy import text

import sqlglot
from sqlglot import expressions as exp

from config import engine_ro
from query_router import router, RouterResult
from guardrails import validate, performance_warnings, GuardrailError, GuardrailReport
from llm_client import generate_sql, LLMError
from semantic_layer import semantic

CONFIDENCE_THRESHOLD = 0.7

COMPLEXITY_KEYWORDS = [
    "сравни", "сравнение", "относительно", "процент", "доля",
    "динамика", "прирост", "рост", "падение", "когорт", "ретенц",
    "медиан", "перцентил", "разниц",
]


@dataclass
class ExecResult:
    data: list[dict]
    rows: int
    columns: list[str]
    error: Optional[str] = None


@dataclass
class DispatchResult:
    query: str
    sql: Optional[str] = None
    route: str = "unknown"
    confidence: float = 0.0
    understanding: dict = field(default_factory=dict)
    guard_report: Optional[dict] = None
    warnings: list[str] = field(default_factory=list)
    llm_latency_ms: Optional[int] = None
    exec_error: Optional[str] = None
    data: list[dict] = field(default_factory=list)
    rows: int = 0
    columns: list[str] = field(default_factory=list)
    status: str = "ok"
    stats_template: Optional[dict] = None

    def to_dict(self) -> dict:
        d = asdict(self)
        return d


def _complexity_signals_in_query(q: str) -> bool:
    low = q.lower()
    return any(k in low for k in COMPLEXITY_KEYWORDS)


_AGG_LABELS = {"COUNT": "COUNT(*)", "SUM": "SUM", "AVG": "AVG", "MIN": "MIN", "MAX": "MAX"}


def _understanding_from_sql(sql: str) -> dict:
    """Поля explain из AST SQL (агрегация, разрез, фильтр, LIMIT)."""
    u: dict = {}
    try:
        tree = sqlglot.parse_one(sql, read="mysql")
    except Exception:
        return u
    if tree is None:
        return u

    aggs = []
    for node in tree.find_all(exp.AggFunc):
        name = type(node).__name__.upper()
        try:
            aggs.append(node.sql(dialect="mysql"))
        except Exception:
            aggs.append(name)
    if aggs:
        u["агрегация"] = ", ".join(dict.fromkeys(aggs))

    groups = []
    for g in tree.find_all(exp.Group):
        for col in g.expressions:
            try:
                groups.append(col.sql(dialect="mysql"))
            except Exception:
                pass
    if groups:
        u["разрез"] = ", ".join(groups)

    where = tree.find(exp.Where)
    if where is not None:
        try:
            raw = where.this.sql(dialect="mysql")
            u["фильтр"] = raw if len(raw) < 200 else raw[:197] + "…"
        except Exception:
            pass

    lim = tree.find(exp.Limit)
    if lim is not None and lim.expression is not None:
        try:
            u["top_n"] = int(lim.expression.sql(dialect="mysql"))
        except Exception:
            pass

    sel_cols = []
    select = tree.find(exp.Select)
    if select is not None:
        for e in select.expressions:
            try:
                sel_cols.append(e.sql(dialect="mysql"))
            except Exception:
                pass
    if sel_cols and "метрика" not in u:
        u["метрика"] = ", ".join(sel_cols[:4])
    return u


def _resolved_term_from_sql(sql: str) -> Optional[str]:
    sql_text = sql or ""
    low = sql_text.lower()
    try:
        tree = sqlglot.parse_one(sql_text, read="mysql")
    except Exception:
        tree = None

    select_aggs: list[str] = []
    if tree is not None:
        sel = tree.find(exp.Select)
        if sel is not None:
            for e in sel.expressions:
                if isinstance(e, exp.Alias):
                    node = e.this
                else:
                    node = e
                if isinstance(node, exp.AggFunc):
                    try:
                        select_aggs.append(node.sql(dialect="mysql").lower().replace(" ", ""))
                    except Exception:
                        pass

    dims = semantic.list_all(kind="dimension")
    for t in dims:
        col = (t.get("column_expr") or "").lower()
        if col and col in low:
            return t["term"]

    metrics = semantic.list_all(kind="metric")
    candidates: list[dict] = []
    for t in metrics:
        agg = (t.get("agg") or "").lower().replace(" ", "")
        if agg and agg in select_aggs:
            candidates.append(t)

    if candidates:
        if len(candidates) == 1:
            return candidates[0]["term"]
        best: Optional[dict] = None
        best_score = 0
        for t in candidates:
            fs = (t.get("filter_sql") or "").strip().lower()
            if not fs:
                continue
            if fs in low:
                sc = len(fs)
            else:
                parts = [p.strip() for p in re.split(r"\s+and\s+", fs, flags=re.I) if p.strip()]
                sc = max((len(p) for p in parts if p in low), default=0)
            if sc > best_score:
                best_score = sc
                best = t
        if best is not None and best_score > 0:
            return best["term"]

    return None


def _run_sql(sql: str) -> ExecResult:
    with engine_ro.connect() as conn:
        result = conn.execute(text(sql))
        rows = [dict(r._mapping) for r in result]
        cols = list(rows[0].keys()) if rows else list(result.keys())
    return ExecResult(data=rows, rows=len(rows), columns=cols)


def _try_llm(query: str, res: DispatchResult) -> Optional[str]:
    try:
        llm = generate_sql(query)
    except LLMError as e:
        res.warnings.append(f"LLM недоступна: {e}")
        res.status = "llm_unavailable"
        return None
    res.llm_latency_ms = llm.latency_ms

    if llm.explanation or llm.logic_path:
        llm_understanding = {}
        if llm.explanation:
            llm_understanding["explanation"] = llm.explanation
        if llm.logic_path:
            llm_understanding["logic_path"] = llm.logic_path
        if llm.stats_template:
            llm_understanding["stats_template"] = llm.stats_template
            res.stats_template = llm.stats_template
        res.understanding.update(llm_understanding)

    return llm.sql


def answer(query: str) -> DispatchResult:
    res = DispatchResult(query=query)

    rr: RouterResult = router.build(query)
    res.understanding = rr.plan.understanding
    if rr.plan.metric:
        res.understanding["resolved_term"] = rr.plan.metric.term
    elif rr.plan.dimensions:
        res.understanding["resolved_term"] = rr.plan.dimensions[0].term

    use_rule = (
        rr.sql is not None
        and rr.confidence >= CONFIDENCE_THRESHOLD
        and not _complexity_signals_in_query(query)
    )
    sql: Optional[str] = rr.sql if use_rule else None
    res.route = "rule" if use_rule else "llm"
    res.confidence = rr.confidence

    if sql is None:
        if not rr.plan.metric and not rr.plan.dimensions:
            res.status = "not_recognized"
            res.route = "rule"
            return res

        sql = _try_llm(query, res)
        if sql is None:
            if not rr.sql:
                res.status = res.status if res.status != "ok" else "not_recognized"
                return res
            sql = rr.sql
            res.route = "rule (LLM unavailable)"

    try:
        report: GuardrailReport = validate(sql)
    except GuardrailError as e:
        res.status = "blocked"
        res.exec_error = f"[{e.code}] {e.reason}"
        res.sql = sql
        return res

    res.sql = report.sql
    res.guard_report = {
        "tables": report.tables, "complexity": report.complexity,
        "has_join": report.has_join, "has_subquery": report.has_subquery,
        "had_limit": report.had_limit, "has_aggregate": report.has_aggregate,
    }
    res.warnings.extend(performance_warnings(report.sql))

    try:
        exec_res = _run_sql(report.sql)
    except Exception as e:
        if res.route == "rule":
            res.warnings.append(f"Rule-SQL упал на выполнении: {type(e).__name__}. Пробую LLM.")
            llm_sql = _try_llm(query, res)
            if llm_sql:
                try:
                    report2 = validate(llm_sql)
                    exec_res = _run_sql(report2.sql)
                    res.sql = report2.sql
                    res.guard_report = {
                        "tables": report2.tables, "complexity": report2.complexity,
                        "has_join": report2.has_join, "has_subquery": report2.has_subquery,
                        "had_limit": report2.had_limit, "has_aggregate": report2.has_aggregate,
                    }
                    res.route = "rule+llm"
                    res.warnings.extend(performance_warnings(report2.sql))
                except (GuardrailError, Exception) as e2:  # noqa: BLE001
                    res.status = "error"
                    res.exec_error = f"{type(e).__name__}: {e}; LLM тоже не удалась: {e2}"
                    return res
            else:
                res.status = "error"
                res.exec_error = f"{type(e).__name__}: {e}"
                return res
        else:
            res.status = "error"
            res.exec_error = f"{type(e).__name__}: {e}"
            return res

    res.data = exec_res.data
    res.rows = exec_res.rows
    res.columns = exec_res.columns

    if res.route.startswith("llm") or res.route.startswith("rule+llm"):
        llm_u = _understanding_from_sql(res.sql or "")
        old_resolved = res.understanding.get("resolved_term")
        resolved_from_sql = _resolved_term_from_sql(res.sql or "")
        if llm_u:
            res.understanding = llm_u
        if resolved_from_sql:
            res.understanding["resolved_term"] = resolved_from_sql
        elif old_resolved:
            res.understanding["resolved_term"] = old_resolved
    return res


def recommend_chart(columns: list[str], rows: int) -> str:
    if rows == 0:
        return "empty"
    if len(columns) == 1:
        return "table"
    if len(columns) == 2:
        first = columns[0].lower()
        if any(m in first for m in ("date", "timestamp", "дат", "день", "month", "месяц")):
            return "line"
        return "bar"
    first = columns[0].lower()
    time_like = any(
        m in first for m in ("date", "timestamp", "дат", "день", "month", "месяц", "year", "год")
    )
    if time_like:
        return "bar"
    return "table"
