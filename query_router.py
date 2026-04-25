"""Rule-router: terms → SQL."""
from __future__ import annotations

import re
from dataclasses import dataclass, field, asdict
from typing import Optional

from semantic_layer import semantic, Term, KIND_METRIC, KIND_DIMENSION
from config import FORCED_LIMIT, DATA_TABLES, PRIMARY_FACT_TABLE

TABLE = PRIMARY_FACT_TABLE

_PERIOD_PATTERNS = [
    (re.compile(r"\b(сегодн|today)"), "DATE(order_timestamp) = CURDATE()", "сегодня"),
    (re.compile(r"\b(вчера|yesterday)"), "DATE(order_timestamp) = CURDATE() - INTERVAL 1 DAY", "вчера"),
    (re.compile(r"прошл\w*\s+недел"), "order_timestamp >= CURDATE() - INTERVAL 7 DAY AND order_timestamp < CURDATE()", "прошлая неделя"),
    (re.compile(r"(за|на)\s+недел"), "order_timestamp >= CURDATE() - INTERVAL 7 DAY", "за неделю"),
    (re.compile(r"прошл\w*\s+месяц"), "YEAR(order_timestamp) = YEAR(CURDATE() - INTERVAL 1 MONTH) AND MONTH(order_timestamp) = MONTH(CURDATE() - INTERVAL 1 MONTH)", "прошлый месяц"),
    (
        re.compile(
            r"(?:\b(?:текущ\w*|этот)\s+месяц\b|\bcurrent\s+month\b|"
            r"\bв\s+этом\s+месяц\w*\b|\bза\s+(?:текущ\w*|этот)\s+месяц\b)"
        ),
        "YEAR(order_timestamp) = YEAR(CURDATE()) AND MONTH(order_timestamp) = MONTH(CURDATE())",
        "текущий месяц",
    ),
    (re.compile(r"\bполгод"), "order_timestamp >= CURDATE() - INTERVAL 6 MONTH", "за полгода"),
    (re.compile(r"\bквартал"), "order_timestamp >= CURDATE() - INTERVAL 3 MONTH", "за квартал"),
    (re.compile(r"(за|на|последн\w*)\s+(\d{1,2})\s+месяц"), None, None),
    (re.compile(r"(за|в)\s+месяц"), "order_timestamp >= CURDATE() - INTERVAL 30 DAY", "за месяц"),
    (re.compile(r"прошл\w*\s+год"), "YEAR(order_timestamp) = YEAR(CURDATE()) - 1", "прошлый год"),
    (re.compile(r"(за|в)\s+год|этот\s+год|текущ\w*\s+год"), "YEAR(order_timestamp) = YEAR(CURDATE())", "текущий год"),
]

_N_MONTHS_RE = re.compile(r"(?:за|на|последн\w*)\s+(\d{1,2})\s+месяц")
_N_MONTHS_LABEL_RE = re.compile(r"за\s+(\d{1,2})\s+мес", re.IGNORECASE)
_N_WEEKS_RE = re.compile(r"(?:за|на|последн\w*)\s+(\d{1,2})\s+недел")
_N_DAYS_RE = re.compile(r"(?:за|на|последн\w*)\s+(\d{1,3})\s+(?:дн|ден)")

_AVG_REQUEST_RE = re.compile(r"\b(в\s+среднем|средн\w*\s+(?:количеств|число|знач))")

_MONTH_MAP = {
    "январ": 1, "февраль": 2, "феврал": 2, "март": 3, "апрел": 4,
    "май": 5, "мае": 5, "июн": 6, "июл": 7, "август": 8,
    "сентябр": 9, "октябр": 10, "ноябр": 11, "декабр": 12,
}

_YEAR_RE = re.compile(r"\b(20\d{2})\b")
_TOPN_RE = re.compile(r"(?:топ[-\s]?|первы[хе]\s+)(\d{1,3})")


@dataclass
class QueryPlan:
    metric: Optional[Term] = None
    dimensions: list[Term] = field(default_factory=list)
    period_sql: Optional[str] = None
    period_label: Optional[str] = None
    top_n: Optional[int] = None
    understanding: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["metric"] = self.metric.to_dict() if self.metric else None
        d["dimensions"] = [t.to_dict() for t in self.dimensions]
        return d


@dataclass
class RouterResult:
    sql: Optional[str]
    confidence: float
    plan: QueryPlan
    route: str = "rule"
    reason: str = ""

    def to_dict(self) -> dict:
        return {
            "sql": self.sql,
            "confidence": round(self.confidence, 3),
            "plan": self.plan.to_dict(),
            "route": self.route,
            "reason": self.reason,
        }


class RuleRouter:

    @staticmethod
    def _resolve_from_table(plan: QueryPlan) -> str:
        parts: list[str] = []
        if plan.metric:
            for x in (plan.metric.column_expr, plan.metric.agg or "", plan.metric.filter_sql or ""):
                parts.append(x)
        for d in plan.dimensions:
            parts.append(d.column_expr)
        blob = " ".join(parts)
        for t in DATA_TABLES:
            if f"{t}." in blob:
                return t
        return TABLE

    @staticmethod
    def _adapt_expr_for_table(from_table: str, expr: Optional[str]) -> Optional[str]:
        if not expr or from_table == "incity":
            return expr
        e = expr
        if from_table == "pass_detail":
            e = e.replace("order_timestamp", "order_date_part")
        elif from_table == "driver_detail":
            e = e.replace("order_timestamp", "tender_date_part")
        return e

    def build(self, query: str) -> RouterResult:
        terms = semantic.find_in_query(query)
        metric = next((t for t in terms if t.kind == KIND_METRIC), None)
        dimensions = [t for t in terms if t.kind == KIND_DIMENSION]

        period_sql, period_label = self._detect_period(query)
        top_n = self._detect_top_n(query)
        low_q = query.lower()

        explicit_month_breakdown = bool(
            re.search(r"по\s+месяц|помесяч|помесячн|ежемесяч", low_q)
        )
        if period_label and "месяц" in period_label and not explicit_month_breakdown:
            dimensions = [d for d in dimensions if not self._is_month_dim(d)]

        if (
            _AVG_REQUEST_RE.search(query.lower())
            and not dimensions
            and metric
            and (metric.agg or "").upper().startswith(("COUNT", "SUM"))
            and period_sql
            and self._period_spans_months(period_label)
        ):
            month_dim = Term(
                term="месяц",
                kind=KIND_DIMENSION,
                column_expr="DATE_FORMAT(order_timestamp, '%Y-%m')",
                synonyms=[],
            )
            dimensions = [month_dim]

        period_sql, period_label, dimensions = self._calendar_month_bucket_for_n_months_window(
            period_sql, period_label, dimensions
        )

        plan = QueryPlan(
            metric=metric,
            dimensions=dimensions,
            period_sql=period_sql,
            period_label=period_label,
            top_n=top_n,
        )
        plan.understanding = self._human_readable(plan)

        sql = self._assemble(plan)
        confidence = self._score(query, plan, sql)

        reason = self._reason(plan, sql)
        return RouterResult(sql=sql, confidence=confidence, plan=plan, reason=reason)

    @staticmethod
    def _is_month_dim(term: Term) -> bool:
        col = (term.column_expr or "").lower()
        t = (term.term or "").lower()
        return "month(" in col or "month" in col or "месяц" in t

    @staticmethod
    def _calendar_month_bucket_for_n_months_window(
        period_sql: Optional[str],
        period_label: Optional[str],
        dimensions: list[Term],
    ) -> tuple[Optional[str], Optional[str], list[Term]]:
        if not period_sql or not period_label:
            return period_sql, period_label, dimensions
        m = _N_MONTHS_LABEL_RE.search(period_label)
        if not m or not any(RuleRouter._is_month_dim(d) for d in dimensions):
            return period_sql, period_label, dimensions
        n = max(1, min(int(m.group(1)), 36))
        new_sql = (
            "order_timestamp >= DATE_SUB(DATE_FORMAT(CURDATE(), '%Y-%m-01'), "
            f"INTERVAL {n - 1} MONTH) "
            "AND order_timestamp < DATE_FORMAT(CURDATE(), '%Y-%m-01') + INTERVAL 1 MONTH"
        )
        new_dims: list[Term] = []
        for d in dimensions:
            col = (d.column_expr or "").lower()
            if RuleRouter._is_month_dim(d) and "date_format" not in col:
                new_dims.append(
                    Term(
                        term=d.term,
                        kind=d.kind,
                        column_expr="DATE_FORMAT(order_timestamp, '%Y-%m')",
                        agg=d.agg,
                        filter_sql=d.filter_sql,
                        synonyms=list(d.synonyms or []),
                        is_user_added=d.is_user_added,
                        id=d.id,
                    )
                )
            else:
                new_dims.append(d)
        return new_sql, period_label, new_dims

    def _detect_period(self, query: str) -> tuple[Optional[str], Optional[str]]:
        q = query.lower()
        m = _N_MONTHS_RE.search(q)
        if m:
            n = max(1, min(int(m.group(1)), 36))
            return f"order_timestamp >= CURDATE() - INTERVAL {n} MONTH", f"за {n} мес."
        m = _N_WEEKS_RE.search(q)
        if m:
            n = max(1, min(int(m.group(1)), 52))
            return f"order_timestamp >= CURDATE() - INTERVAL {n} WEEK", f"за {n} нед."
        m = _N_DAYS_RE.search(q)
        if m:
            n = max(1, min(int(m.group(1)), 365))
            return f"order_timestamp >= CURDATE() - INTERVAL {n} DAY", f"за {n} дн."
        for rx, sql, label in _PERIOD_PATTERNS:
            if sql is None:
                continue
            if rx.search(q):
                return sql, label
        for stem, num in _MONTH_MAP.items():
            if stem in q:
                year_match = _YEAR_RE.search(q)
                year = int(year_match.group(1)) if year_match else None
                if year:
                    return (f"YEAR(order_timestamp) = {year} AND MONTH(order_timestamp) = {num}",
                            f"{stem[:3]}. {year}")
                return (f"MONTH(order_timestamp) = {num}", f"месяц {num}")
        m = _YEAR_RE.search(q)
        if m:
            return f"YEAR(order_timestamp) = {m.group(1)}", m.group(1)
        return None, None

    def _detect_top_n(self, query: str) -> Optional[int]:
        m = _TOPN_RE.search(query.lower())
        if m:
            n = int(m.group(1))
            return max(1, min(n, 1000))
        return None

    def _assemble(self, plan: QueryPlan) -> Optional[str]:
        if not plan.metric and not plan.dimensions:
            return None

        from_table = self._resolve_from_table(plan)

        select_parts: list[str] = []
        group_parts: list[str] = []

        for dim in plan.dimensions:
            dexpr = self._adapt_expr_for_table(from_table, dim.column_expr) or dim.column_expr
            alias = self._alias_for(dexpr)
            select_parts.append(f"{dexpr} AS {alias}")
            group_parts.append(dexpr)

        agg_alias = None
        if plan.metric:
            if plan.metric.agg:
                agg_alias = self._metric_alias(plan.metric)
                agg_e = self._adapt_expr_for_table(from_table, plan.metric.agg) or plan.metric.agg
                select_parts.append(f"{agg_e} AS {agg_alias}")
            else:
                agg_alias = "count"
                select_parts.append("COUNT(*) AS count")
        elif plan.dimensions:
            agg_alias = "count"
            select_parts.append("COUNT(*) AS count")

        where_parts: list[str] = []
        if plan.metric and plan.metric.filter_sql:
            fe = self._adapt_expr_for_table(from_table, plan.metric.filter_sql) or plan.metric.filter_sql
            where_parts.append(f"({fe})")
        for dim in plan.dimensions:
            dexpr = self._adapt_expr_for_table(from_table, dim.column_expr) or dim.column_expr
            where_parts.append(f"{dexpr} IS NOT NULL")
        if plan.period_sql:
            pe = self._adapt_expr_for_table(from_table, plan.period_sql) or plan.period_sql
            where_parts.append(f"({pe})")

        sql = "SELECT " + ", ".join(select_parts) + f"\nFROM {from_table}"
        if where_parts:
            sql += "\nWHERE " + "\n  AND ".join(where_parts)
        if group_parts:
            sql += "\nGROUP BY " + ", ".join(group_parts)

        order_col = self._choose_order(plan, agg_alias, from_table=from_table)
        if order_col:
            sql += f"\nORDER BY {order_col}"

        limit = plan.top_n or FORCED_LIMIT
        sql += f"\nLIMIT {limit}"
        return sql

    def _choose_order(
        self, plan: QueryPlan, agg_alias: Optional[str], *, from_table: str
    ) -> Optional[str]:
        for dim in plan.dimensions:
            dex = self._adapt_expr_for_table(from_table, dim.column_expr) or dim.column_expr
            if "order_timestamp" in dex or "order_date_part" in dex or "tender_date_part" in dex:
                return self._alias_for(dex) + " ASC"
        if agg_alias:
            return f"{agg_alias} DESC"
        return None

    @staticmethod
    def _alias_for(column_expr: str) -> str:
        if "DATE_FORMAT" in column_expr.upper():
            fmt = re.search(r"'([^']+)'", column_expr)
            if fmt and "%m" in fmt.group(1) and "%d" not in fmt.group(1):
                return "month"
            if fmt and "%Y" in fmt.group(1) and "%m" not in fmt.group(1):
                return "year"
            return "period"
        inner = re.search(r"\(([^)]+)\)", column_expr)
        if inner:
            parts = re.findall(r"[A-Za-z_][A-Za-z0-9_]*", column_expr)
            if parts:
                func = parts[0].lower()
                return "date" if func in ("date", "day") else func
        return column_expr

    @staticmethod
    def _period_spans_months(label: Optional[str]) -> bool:
        if not label:
            return False
        l = label.lower()
        if any(k in l for k in ("полгода", "квартал", "год")):
            return True
        m = re.search(r"за\s+(\d{1,2})\s+мес", l)
        if m and int(m.group(1)) >= 2:
            return True
        return False

    @staticmethod
    def _metric_alias(metric: Term) -> str:
        agg = (metric.agg or "").upper()
        if agg.startswith("COUNT"):
            return metric.term.split()[0]
        if agg.startswith("SUM"):
            return "total"
        if agg.startswith("AVG"):
            return "avg_value"
        return "value"

    def _score(self, query: str, plan: QueryPlan, sql: Optional[str]) -> float:
        if sql is None:
            return 0.15
        score = 0.0
        if plan.metric:
            score += 0.45
        if plan.dimensions:
            score += 0.30
        if plan.period_sql:
            score += 0.15
        if plan.top_n:
            score += 0.10

        complexity_signals = ["сравни", "относительно", "по сравнению", "процент", "доля",
                              "медиан", "рост", "падение", "динамика", "как меняет"]
        for w in complexity_signals:
            if w in query.lower():
                score -= 0.25
                break

        noise_signals = ["сравни", "относительно", "по сравнению", "процент", "доля",
                         "медиан", "рост", "падение"]
        for w in noise_signals:
            if w in query.lower():
                score -= 0.15
                break
        return max(0.0, min(1.0, score))

    def _reason(self, plan: QueryPlan, sql: Optional[str]) -> str:
        if not sql:
            if not plan.metric and not plan.dimensions:
                return "Не распознаны ни метрика, ни измерение"
            return "Не удалось собрать SQL"
        parts = []
        if plan.metric:
            parts.append(f"метрика={plan.metric.term}")
        if plan.dimensions:
            parts.append(f"разрез={', '.join(d.term for d in plan.dimensions)}")
        if plan.period_label:
            parts.append(f"период={plan.period_label}")
        if plan.top_n:
            parts.append(f"top={plan.top_n}")
        return "; ".join(parts)

    def _human_readable(self, plan: QueryPlan) -> dict:
        return {
            "метрика": plan.metric.term if plan.metric else None,
            "агрегация": plan.metric.agg if plan.metric else None,
            "фильтр": plan.metric.filter_sql if plan.metric else None,
            "разрез": ", ".join(d.term for d in plan.dimensions) or None,
            "период": plan.period_label,
            "top_n": plan.top_n,
        }


router = RuleRouter()
