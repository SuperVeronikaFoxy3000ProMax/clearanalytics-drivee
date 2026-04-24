"""FastAPI бэкенд NL→SQL-аналитики."""
from __future__ import annotations

from datetime import datetime

from pathlib import Path

from fastapi import FastAPI, HTTPException, Body
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
import pandas as pd
import plotly.express as px

from config import engine_ro
from semantic_layer import semantic
from query_logger import log_query, tail as tail_logs
from dispatcher import answer as dispatch_answer, recommend_chart
from guardrails import validate, GuardrailError
from llm_client import healthcheck as llm_health

app = FastAPI(title="Drivee NL→SQL", version="0.2.0")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
)
engine = engine_ro

STATIC_DIR = Path(__file__).parent
saved_reports: list[dict] = []


@app.get("/", response_class=HTMLResponse)
def index():
    return FileResponse(STATIC_DIR / "app.html")


@app.get("/app.css")
def app_css():
    return FileResponse(STATIC_DIR / "app.css", media_type="text/css")


@app.get("/ask")
def ask(query: str):
    with log_query(query) as log:
        res = dispatch_answer(query)
        log.route_type = res.route
        log.sql = res.sql
        log.confidence = res.confidence
        log.rows = res.rows
        log.status = res.status
        if res.exec_error:
            log.error = res.exec_error

        if res.status != "ok":
            return {
                "status": res.status,
                "query": query,
                "error": res.exec_error,
                "understanding": res.understanding,
                "confidence": res.confidence,
                "sql": res.sql,
                "warnings": res.warnings,
                "route": res.route,
            }

        saved_reports.append({
            "timestamp": datetime.now().isoformat(),
            "query": query, "sql": res.sql,
            "understanding": res.understanding,
            "rows_count": res.rows, "confidence": res.confidence,
            "route": res.route,
        })

        return {
            "status": "ok",
            "query": query,
            "understanding": res.understanding,
            "sql": res.sql,
            "data": res.data,
            "rows": res.rows,
            "columns": res.columns,
            "confidence": res.confidence,
            "route": res.route,
            "llm_latency_ms": res.llm_latency_ms,
            "guard": res.guard_report,
            "warnings": res.warnings,
            "recommended_chart": recommend_chart(res.columns, res.rows),
            "stats_template": res.stats_template,  # описания для KPI-блока от LLM
        }


@app.get("/query")
def run_query(sql: str):
    try:
        report = validate(sql)
    except GuardrailError as e:
        raise HTTPException(400, {"code": e.code, "reason": e.reason})
    try:
        with engine.connect() as conn:
            result = conn.execute(text(report.sql))
            data = [dict(r._mapping) for r in result]
        return {"sql": report.sql, "rows": len(data), "data": data}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/visualize")
def visualize(query: str, chart_type: str = "auto"):
    res = dispatch_answer(query)
    if res.status != "ok" or not res.data:
        raise HTTPException(400, res.exec_error or "Нет данных")
    df = pd.DataFrame(res.data)
    if chart_type == "auto":
        chart_type = recommend_chart(res.columns, res.rows)
    fig = _build_fig(df, chart_type)
    return {"sql": res.sql, "chart_type": chart_type,
            "chart": fig.to_json() if fig else None,
            "data": res.data, "understanding": res.understanding}


@app.get("/chart", response_class=HTMLResponse)
def show_chart(query: str, chart_type: str = "auto"):
    res = dispatch_answer(query)
    if res.status != "ok" or not res.data:
        return f"<h1>Ошибка</h1><p>{res.exec_error or 'Нет данных'}</p>"
    df = pd.DataFrame(res.data)
    if chart_type == "auto":
        chart_type = recommend_chart(res.columns, res.rows)
    fig = _build_fig(df, chart_type)
    fig_html = fig.to_html(include_plotlyjs=False, full_html=False) if fig else "<p>—</p>"
    return f"""
    <html><head><meta charset="utf-8">
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script></head>
    <body>
      <h2>{query}</h2>
      <p><b>SQL:</b><pre><code>{res.sql}</code></pre></p>
      {fig_html}
    </body></html>
    """


def _build_fig(df: pd.DataFrame, chart_type: str):
    if len(df.columns) == 0:
        return None
    x = str(df.columns[0])
    y = str(df.columns[1]) if len(df.columns) > 1 else x
    if chart_type == "line":
        return px.line(df.sort_values(by=x), x=x, y=y, markers=True, title=f"{y} по {x}")
    if chart_type == "pie":
        return px.pie(df, names=x, values=y, title=f"Распределение {y}")
    if chart_type == "kpi":
        return px.bar(df, x=x, y=y, title=f"{y}")
    if chart_type == "table":
        return None
    return px.bar(df, x=x, y=y, title=f"{y} по {x}")


@app.get("/reports")
def get_reports():
    return {"total": len(saved_reports), "reports": saved_reports}


@app.get("/logs")
def get_logs(limit: int = 50):
    return {"entries": tail_logs(limit)}


@app.get("/semantic-layer")
def get_semantic_layer(kind: str | None = None):
    return {"total": len(semantic.list_all()), "terms": semantic.list_all(kind=kind)}


@app.post("/semantic-layer")
def add_semantic_term(payload: dict = Body(...)):
    required = ("term", "kind", "column_expr")
    missing = [f for f in required if not payload.get(f)]
    if missing:
        raise HTTPException(400, f"Отсутствуют поля: {missing}")
    try:
        t = semantic.add(
            term=payload["term"], kind=payload["kind"],
            column_expr=payload["column_expr"],
            agg=payload.get("agg"), filter_sql=payload.get("filter_sql"),
            synonyms=payload.get("synonyms") or [],
            actor=payload.get("actor", "user"),
        )
        return {"ok": True, "term": t.to_dict()}
    except ValueError as e:
        raise HTTPException(400, str(e))


@app.post("/semantic-layer/synonym")
def add_synonym(payload: dict = Body(...)):
    term = payload.get("term"); synonym = payload.get("synonym")
    if not term or not synonym:
        raise HTTPException(400, "Нужны поля term и synonym")
    added = semantic.add_synonym(term, synonym, actor=payload.get("actor", "user"))
    return {"ok": added, "term": term, "synonym": synonym}


@app.delete("/semantic-layer/{term}")
def delete_term(term: str):
    if not semantic.delete(term):
        raise HTTPException(404, "Термин не найден")
    return {"ok": True}


@app.get("/semantic-layer/history")
def semantic_history(limit: int = 100):
    return {"history": semantic.history(limit=limit)}


TEMPLATES = [
    {"id": "cancels_by_city", "title": "Отмены по городам за неделю",
     "example_query": "отмены по городам за прошлую неделю",
     "description": "Сколько отменённых заказов в каждом городе за последние 7 дней.",
     "chart": "bar"},
    {"id": "revenue_by_month", "title": "Выручка по месяцам",
     "example_query": "выручка по месяцам в 2025",
     "description": "SUM(price_order_local) по завершённым заказам, группировка по MONTH.",
     "chart": "line"},
    {"id": "top_drivers", "title": "Топ-10 водителей",
     "example_query": "топ 10 водителей по количеству поездок",
     "description": "Десять водителей с наибольшим числом завершённых поездок.",
     "chart": "bar"},
    {"id": "orders_by_status", "title": "Заказы по статусам",
     "example_query": "статистика по статусам",
     "description": "Распределение всех заказов по status_order.",
     "chart": "pie"},
    {"id": "avg_price_by_status", "title": "Средний чек по статусам",
     "example_query": "средний чек по статусам",
     "description": "AVG(price_order_local) в разрезе статусов.",
     "chart": "bar"},
]


@app.get("/templates")
def get_templates():
    return {"total": len(TEMPLATES), "templates": TEMPLATES}


@app.get("/health")
def health():
    db_ok = False
    db_err = None
    try:
        with engine_ro.connect() as c:
            c.execute(text("SELECT 1")).scalar()
            db_ok = True
    except Exception as e:
        db_err = str(e)
    return {"db": {"ok": db_ok, "error": db_err}, "llm": llm_health()}
