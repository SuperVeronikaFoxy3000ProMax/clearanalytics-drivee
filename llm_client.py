"""LM Studio HTTP client."""
from __future__ import annotations

import re
import time
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional

import httpx
import sqlglot
from sqlalchemy import text

from config import (
    engine_admin, DB_NAME, DATA_TABLES, LLM_BASE_URL, LLM_MODEL, LLM_TIMEOUT_SEC,
    LLM_MAX_TOKENS, LLM_TEMPERATURE,
)
from semantic_layer import semantic


class LLMError(RuntimeError):
    """LLM недоступна или вернула невалидный ответ."""


@dataclass
class LLMResult:
    sql: str
    raw: str
    latency_ms: int
    model: str
    explanation: Optional[str] = None
    logic_path: Optional[str] = None
    stats_template: Optional[dict] = None


@lru_cache(maxsize=1)
def _schema_ddl() -> str:
    blocks: list[str] = []
    for table in DATA_TABLES:
        with engine_admin.connect() as c:
            rows = c.execute(
                text("""
                    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_COMMENT
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = :db AND TABLE_NAME = :t
                    ORDER BY ORDINAL_POSITION
                """),
                {"db": DB_NAME, "t": table},
            ).fetchall()
        lines = [f"CREATE TABLE {table} ("]
        if not rows:
            lines.append("    -- no columns discovered")
        else:
            for name, dtype, nullable, comment in rows:
                nn = "" if nullable == "YES" else " NOT NULL"
                cm = f"  -- {comment}" if comment else ""
                lines.append(f"    {name} {dtype.upper()}{nn},{cm}".rstrip(","))
            lines[-1] = lines[-1].rstrip(",")
        lines.append(");")
        blocks.append("\n".join(lines))
    return "\n\n".join(blocks)


def _semantic_hints() -> str:
    parts = []
    for t in semantic.list_all():
        bits = [f"- '{t['term']}'"]
        if t["kind"] == "metric":
            bits.append(f"({t['agg']})")
            if t["filter_sql"]:
                bits.append(f"filter: {t['filter_sql']}")
        elif t["kind"] == "dimension":
            bits.append(f"group by {t['column_expr']}")
        if t["synonyms"]:
            bits.append(f"synonyms: {', '.join(t['synonyms'][:4])}")
        parts.append(" ".join(bits))
    return "\n".join(parts)


def build_prompt(question: str) -> str:
    return (
        "### Task\n"
        "Generate a single MySQL SELECT query that answers the user's question.\n"
        "Return your answer as a valid JSON object with keys: sql, explanation, logic_path, stats_template.\n"
        "Rules:\n"
        "- Only SELECT; no INSERT/UPDATE/DELETE/DDL.\n"
        "- Only these tables exist: `incity` (детальные заказы/тендеры), "
        "`pass_detail` (дневные метрики пассажиров), `driver_detail` (дневные метрики водителей). "
        "Join only when the question requires it; prefer the smallest set of tables.\n"
        "- Always include LIMIT (<= 1000).\n"
        "- When calculating metrics (AVG/SUM/COUNT), ALWAYS add GROUP BY with relevant dimensions.\n"
        "- For aggregates, include at least one dimension column (city, status, month, driver, etc.) to show breakdown.\n"
        "- Example: for 'average duration' add GROUP BY city_order or status_order to show detailed data.\n"
        "- Never return just a single aggregate value without dimensions - users need detailed breakdown.\n"
        "- Prefer simplest query that returns the answer with meaningful grouping.\n"
        "- Russian terms map to columns per the business glossary below.\n"
        "\n"
        "### Time periods — CRITICAL\n"
        "- **Current calendar month** (Russian: «текущий месяц», «этот месяц», «за текущий месяц», «в этом месяце»): "
        "filter with `YEAR(order_timestamp) = YEAR(CURDATE()) AND MONTH(order_timestamp) = MONTH(CURDATE())` "
        "(or `order_timestamp >= DATE_FORMAT(CURDATE(), '%Y-%m-01') AND order_timestamp < DATE_FORMAT(CURDATE(), '%Y-%m-01') + INTERVAL 1 MONTH`). "
        "Do **not** use only `MONTH(order_timestamp)` in SELECT/GROUP BY for that — it merges January..December across all years.\n"
        "- **Trend by month** (explicit: «по месяцам», «помесячно», «динамика по месяцам»): use `DATE_FORMAT(order_timestamp, '%Y-%m')` or both YEAR and MONTH in GROUP BY, with a sensible date range in WHERE.\n"
        "- **«За N месяцев» + ряд по месяцам**: in WHERE use **exactly N календарных месяцев, включая текущий**: "
        "`order_timestamp >= DATE_SUB(DATE_FORMAT(CURDATE(), '%Y-%m-01'), INTERVAL N-1 MONTH) "
        "AND order_timestamp < DATE_FORMAT(CURDATE(), '%Y-%m-01') + INTERVAL 1 MONTH`, "
        "and `GROUP BY DATE_FORMAT(order_timestamp, '%Y-%m')`. "
        "Avoid `>= CURRENT_DATE - INTERVAL N MONTH` together with only `MONTH(...)` — the sliding window often crosses **more than N** month numbers and `MONTH()` drops the year.\n"
        "\n"
        "### OUTPUT FORMAT (JSON)\n"
        "{\n"
        '  "sql": "SELECT ...",\n'
        '  "explanation": "A warm, conversational explanation in Russian — like a smart colleague telling you what they\'re doing. Use natural language, avoid SQL terms. Example: \"Смотрю на завершённые поездки за последние полгода и считаю, сколько в среднем длилась каждая — разбиваю по месяцам, чтобы видеть динамику.\"",\n'
        '  "logic_path": "Slice: [GROUP_BY_COLUMN], Metric: [AGGREGATE_FUNCTION], Filter: [WHERE_CONDITIONS]",\n'
        '  "stats_template": {\n'
        '    "description_sum": "Description of what SUM represents in this context",\n'
        '    "description_avg": "Description of what AVG represents",\n'
        '    "description_max": "Description of what MAX represents"\n'
        "  }\n"
        "}\n"
        "\n"
        "### LOGIC EXPLANATION RULES - CRITICAL!\n"
        "- **FIRST** identify the Slice (Разрез) - the GROUP BY column (e.g., DATE(order_timestamp), city_order).\n"
        "- **SECOND** identify the Metric (Метрика) - the aggregation (e.g., AVG(duration_in_seconds)).\n"
        "- CORRECT format: 'Slice: DATE(order_timestamp), Metric: AVG(duration_in_seconds)'\n"
        "- WRONG format: 'Metric: AVG(...), Slice: DATE(...)'\n"
        "- The Slice MUST come before Metric in logic_path!\n"
        "- For stats_template: write user-friendly descriptions in RUSSIAN like 'Сумма всех отмен за выбранный период' or 'Среднее количество отмен на месяц'\n"
        "- For explanation: Write in RUSSIAN, be warm and conversational — like a smart colleague explaining what they're doing. Avoid SQL jargon (no 'AVG', 'GROUP BY', 'WHERE'). Instead say things like 'смотрю только на завершённые поездки', 'разбиваю по месяцам чтобы видеть динамику', 'беру последние 3 месяца'. Keep it to 1-2 sentences max.\n"
        "\n"
        "### Database Schema\n"
        f"{_schema_ddl()}\n"
        "\n"
        "### Business Glossary\n"
        f"{_semantic_hints()}\n"
        "\n"
        "### Question\n"
        f"{question}\n"
        "\n"
        "### JSON Response\n"
    )


_FENCE_RE = re.compile(r"```(?:sql)?\s*(.*?)```", re.IGNORECASE | re.DOTALL)


def extract_sql(raw: str) -> str:
    if not raw:
        raise LLMError("Пустой ответ LLM")
    m = _FENCE_RE.search(raw)
    candidate = m.group(1) if m else raw
    candidate = re.split(r"###\s", candidate, maxsplit=1)[0]
    candidate = candidate.strip().rstrip(";").strip()
    if not candidate:
        raise LLMError("Не удалось извлечь SQL из ответа")
    return candidate


_NESTED_INTERVAL_RE = re.compile(
    r"INTERVAL\s*\(\s*INTERVAL\s+'?(\d+)'?\s+(\w+)\s*\)\s*\w+",
    re.IGNORECASE,
)
_INTERVAL_QUOTED_RE = re.compile(r"INTERVAL\s+'(\d+)'\s+(\w+)", re.IGNORECASE)
_ORDER_BY_NULLS_ALIAS_RE = re.compile(
    r"ORDER\s+BY\s+CASE\s+WHEN\s+([A-Za-z_][A-Za-z0-9_]*)\s+IS\s+NULL\s+THEN\s+1\s+ELSE\s+0\s+END\s+DESC\s*,\s*\1\s+DESC",
    re.IGNORECASE,
)


def _fix_mariadb_quirks(sql: str) -> str:
    sql = _NESTED_INTERVAL_RE.sub(r"INTERVAL \1 \2", sql)
    sql = _INTERVAL_QUOTED_RE.sub(r"INTERVAL \1 \2", sql)
    sql = _ORDER_BY_NULLS_ALIAS_RE.sub(r"ORDER BY \1 DESC", sql)
    return sql


def normalize_to_mariadb(sql: str) -> str:
    try:
        out = sqlglot.transpile(sql, read="postgres", write="mysql", pretty=False)
        sql = out[0] if out else sql
    except Exception:
        pass
    return _fix_mariadb_quirks(sql)


def _client() -> httpx.Client:
    return httpx.Client(base_url=LLM_BASE_URL, timeout=LLM_TIMEOUT_SEC)


def generate_sql(question: str) -> LLMResult:
    prompt = build_prompt(question)
    t0 = time.monotonic()
    try:
        with _client() as c:
            resp = c.post(
                "/chat/completions",
                json={
                    "model": LLM_MODEL,
                    "messages": [
                        {"role": "system",
                         "content": "You are a senior SQL analyst. Output only the SQL query, no commentary."},
                        {"role": "user", "content": prompt},
                    ],
                    "temperature": LLM_TEMPERATURE,
                    "max_tokens": LLM_MAX_TOKENS,
                    "stream": False,
                },
            )
    except httpx.ConnectError as e:
        raise LLMError(f"LM Studio недоступен по {LLM_BASE_URL}: {e}")
    except httpx.TimeoutException:
        raise LLMError(f"LLM таймаут > {LLM_TIMEOUT_SEC}с")

    if resp.status_code != 200:
        raise LLMError(f"LLM HTTP {resp.status_code}: {resp.text[:200]}")

    data = resp.json()
    try:
        content = data["choices"][0]["message"]["content"]
    except (KeyError, IndexError) as e:
        raise LLMError(f"Некорректная форма ответа: {e}")

    explanation = None
    logic_path = None
    stats_template = None

    json_match = re.search(r'\{[\s\S]*\}', content)
    if json_match:
        try:
            import json as json_lib
            parsed = json_lib.loads(json_match.group(0))
            sql_raw = parsed.get("sql", "")
            explanation = parsed.get("explanation")
            logic_path = parsed.get("logic_path")
            stats_template = parsed.get("stats_template")
        except Exception:
            sql_raw = extract_sql(content)
    else:
        sql_raw = extract_sql(content)
    
    sql = normalize_to_mariadb(sql_raw)
    return LLMResult(
        sql=sql,
        raw=content,
        latency_ms=int((time.monotonic() - t0) * 1000),
        model=data.get("model", LLM_MODEL),
        explanation=explanation,
        logic_path=logic_path,
        stats_template=stats_template,
    )


def healthcheck() -> dict:
    """Быстрая проверка доступности LM Studio — для /health эндпоинта."""
    try:
        with _client() as c:
            r = c.get("/models", timeout=3)
        if r.status_code == 200:
            models = [m.get("id") for m in r.json().get("data", [])]
            return {"ok": True, "models": models, "url": LLM_BASE_URL}
        return {"ok": False, "status": r.status_code, "url": LLM_BASE_URL}
    except Exception as e:
        return {"ok": False, "error": str(e), "url": LLM_BASE_URL}
