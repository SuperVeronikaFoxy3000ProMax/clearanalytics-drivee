"""FastAPI: /ask, /query, визуализация, словарь, отчёты, логи, health."""
from __future__ import annotations

import html
import io
import logging
import os
import struct
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from project_env import load_project_env

load_project_env()

from fastapi import FastAPI, HTTPException, Body
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
import pandas as pd
import plotly.express as px
import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from config import engine_ro
from semantic_layer import semantic
from query_logger import log_query, tail as tail_logs
from dispatcher import answer as dispatch_answer, recommend_chart
from guardrails import validate, GuardrailError
from llm_client import healthcheck as llm_health
from reports_db import (
    list_reports as db_list_reports,
    get_report as db_get_report,
    add_report as db_add_report,
    update_report as db_update_report,
    delete_report as db_delete_report,
    save_chat_id as db_save_chat_id,
    get_chat_id as db_get_chat_id,
    mark_sent as db_mark_report_sent,
    to_cron as report_to_cron,
    get_default_telegram_user_id as db_default_telegram_user_id,
    last_dispatch_at_for_telegram_user as db_last_dispatch_at,
    get_app_delivery_settings as db_get_app_delivery_settings,
    save_app_delivery_settings as db_save_app_delivery_settings,
    monthly_want_day_int as db_monthly_want_day,
    monthly_effective_dom as db_monthly_effective_dom,
)

log = logging.getLogger(__name__)
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()

MSK_TZ = ZoneInfo("Europe/Moscow")

_scheduler: AsyncIOScheduler | None = None
_registered_jobs: dict[int, str] = {}


async def _send_telegram(chat_id: int, text: str) -> None:
    if not BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.post(url, json={"chat_id": chat_id, "text": text})
        resp.raise_for_status()


def _report_output_format(report: dict) -> str:
    raw = str(report.get("format") or "pdf").strip().lower()
    if raw in ("png",):
        return "png"
    if raw in ("pdf",):
        return "pdf"
    if raw in ("csv",):
        return "csv"
    if raw in ("xlsx", "xls", "excel"):
        return "xlsx"
    return "pdf"


def _telegram_caption(report: dict, res) -> str:
    name = report.get("name") or "Отчёт"
    q = str(report.get("query") or "").strip()
    if len(q) > 220:
        q = q[:217] + "…"
    return f"📊 {name}\nЗапрос: {q}\nСтрок: {res.rows}"


def _text_preview_table(res) -> str:
    lines: list[str] = []
    if res.data:
        cols = list(res.data[0].keys())
        lines.append(" | ".join(cols))
        for row in res.data[:15]:
            lines.append(" | ".join(str(row.get(c, "")) for c in cols))
    return "\n".join(lines)


def _fig_to_static_bytes(fig, fmt: str) -> bytes | None:
    try:
        buf = io.BytesIO()
        fig.write_image(buf, format=fmt, width=980, height=560, scale=1)
        return buf.getvalue()
    except Exception as e:
        log.warning("Plotly write_image(%s) failed: %s", fmt, e)
        return None


def _png_dimensions(png: bytes) -> tuple[int, int]:
    if len(png) >= 24 and png[:8] == b"\x89PNG\r\n\x1a\n":
        w, h = struct.unpack(">II", png[16:24])
        if w > 0 and h > 0:
            return w, h
    return 980, 560


def _wrap_sql_lines(sql: str, width: int = 100) -> str:
    out: list[str] = []
    for line in sql.splitlines():
        while len(line) > width:
            out.append(line[:width])
            line = line[width:]
        out.append(line)
    return "\n".join(out)


_rlab_body_mono: tuple[str, str] | None = None


def _reportlab_body_mono_fonts() -> tuple[str, str]:
    global _rlab_body_mono
    if _rlab_body_mono is not None:
        return _rlab_body_mono
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont

    windir = os.environ.get("WINDIR", r"C:\Windows")
    sans_paths = [
        Path(windir) / "Fonts" / "arial.ttf",
        Path(windir) / "Fonts" / "arialuni.ttf",
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
        Path("/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf"),
        Path("/Library/Fonts/Arial.ttf"),
    ]
    mono_paths = [
        Path(windir) / "Fonts" / "consola.ttf",
        Path(windir) / "Fonts" / "cour.ttf",
        Path("/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf"),
        Path("/usr/share/fonts/truetype/liberation/LiberationMono-Regular.ttf"),
    ]
    body = "Helvetica"
    mono = "Courier"
    for p in sans_paths:
        if not p.is_file():
            continue
        try:
            if "IDRptSans" not in pdfmetrics.getRegisteredFontNames():
                pdfmetrics.registerFont(TTFont("IDRptSans", str(p)))
            body = "IDRptSans"
            break
        except Exception:
            continue
    for p in mono_paths:
        if not p.is_file():
            continue
        try:
            if "IDRptMono" not in pdfmetrics.getRegisteredFontNames():
                pdfmetrics.registerFont(TTFont("IDRptMono", str(p)))
            mono = "IDRptMono"
            break
        except Exception:
            continue
    if mono == "Courier" and body == "IDRptSans":
        mono = body
    _rlab_body_mono = (body, mono)
    return _rlab_body_mono


def _build_report_pdf_with_sql(report: dict, png_bytes: bytes, sql: str) -> bytes:
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import cm
    from reportlab.platypus import Image, PageBreak, Paragraph, Preformatted, SimpleDocTemplate, Spacer

    body_font, mono_font = _reportlab_body_mono_fonts()
    styles = getSampleStyleSheet()
    title_s = ParagraphStyle(
        "rpt_title",
        parent=styles["Heading1"],
        fontName=body_font,
        fontSize=14,
        leading=18,
    )
    label_s = ParagraphStyle(
        "rpt_label",
        parent=styles["Normal"],
        fontName=body_font,
        fontSize=11,
        leading=14,
    )
    sql_s = ParagraphStyle(
        "rpt_sql",
        parent=styles["Code"],
        fontName=mono_font,
        fontSize=8,
        leading=10,
    )

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        rightMargin=1.5 * cm,
        leftMargin=1.5 * cm,
        topMargin=1.5 * cm,
        bottomMargin=1.5 * cm,
    )
    name = str(report.get("name") or "Отчёт").strip()
    sql_text = sql.strip() or "— (SQL не сформирован при выполнении)"
    story: list = [
        Paragraph(html.escape(name).replace("\n", "<br/>"), title_s),
        Spacer(1, 0.35 * cm),
        Paragraph(html.escape("SQL").replace("\n", "<br/>"), label_s),
        Spacer(1, 0.15 * cm),
        Preformatted(_wrap_sql_lines(sql_text), sql_s),
        PageBreak(),
    ]
    pw, ph = A4[0] - 3 * cm, A4[1] - 3 * cm
    iw, ih = _png_dimensions(png_bytes)
    scale = min(pw / float(iw), ph / float(ih))
    nw, nh = iw * scale, ih * scale
    story.append(Image(io.BytesIO(png_bytes), width=nw, height=nh))
    doc.build(story)
    return buf.getvalue()


def _report_telegram_pdf_bytes(fig, report: dict, res) -> bytes | None:
    sql = (res.sql or "").strip() or str(report.get("sql") or "").strip()
    png = _fig_to_static_bytes(fig, "png")
    if not png:
        return _fig_to_static_bytes(fig, "pdf")
    try:
        return _build_report_pdf_with_sql(report, png, sql)
    except Exception as e:
        log.warning("reportlab PDF (SQL+chart) failed: %s", e)
        return _fig_to_static_bytes(fig, "pdf")


def _df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    buf = io.StringIO()
    df.to_csv(buf, index=False, encoding="utf-8-sig")
    return buf.getvalue().encode("utf-8-sig")


def _df_to_xlsx_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Данные")
    return buf.getvalue()


async def _send_telegram_photo(chat_id: int, png_bytes: bytes, caption: str) -> None:
    if not BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"
    cap = (caption or "")[:1024]
    form = {"chat_id": str(chat_id)}
    if cap:
        form["caption"] = cap
    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.post(
            url,
            data=form,
            files={"photo": ("chart.png", png_bytes, "image/png")},
        )
        resp.raise_for_status()


async def _send_telegram_document(
    chat_id: int, filename: str, data: bytes, mime: str, caption: str = ""
) -> None:
    if not BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"
    cap = (caption or "")[:1024]
    form: dict = {"chat_id": str(chat_id)}
    if cap:
        form["caption"] = cap
    async with httpx.AsyncClient(timeout=120) as client:
        resp = await client.post(
            url,
            data=form,
            files={"document": (filename, data, mime)},
        )
        resp.raise_for_status()


async def _run_scheduled_report(report_id: int) -> None:
    report = db_get_report(report_id)
    if not report or not int(report.get("schedule_enabled", 0)):
        return
    freq = str(report.get("freq") or "weekly")
    if freq == "monthly":
        now = datetime.now(MSK_TZ)
        want = db_monthly_want_day(report.get("day"))
        eff = db_monthly_effective_dom(now.year, now.month, want)
        if now.day != eff:
            return
    if report.get("channel") not in ("telegram", "both"):
        return
    tg_user_id = report.get("telegram_user_id")
    if not tg_user_id:
        tg_user_id = db_default_telegram_user_id()
    if not tg_user_id:
        log.warning("Report %s: нет получателя Telegram (ни в отчёте, ни после /start у бота)", report_id)
        return
    chat_id = db_get_chat_id(int(tg_user_id))
    if chat_id is None:
        chat_id = int(tg_user_id)

    res = dispatch_answer(str(report.get("query") or ""))
    if res.status != "ok":
        log.warning("Scheduled report %s failed: %s", report_id, res.exec_error)
        return

    caption = _telegram_caption(report, res)
    out_fmt = _report_output_format(report)

    if not res.data:
        await _send_telegram(chat_id, caption + "\n\nНет строк для выгрузки.")
        db_mark_report_sent(report_id)
        return

    df = pd.DataFrame(res.data)
    chart_type = str(report.get("chart_type") or "auto").strip()
    if chart_type == "auto":
        chart_type = recommend_chart(res.columns, res.rows)
    fig = _build_fig(df, chart_type)

    if out_fmt == "png":
        if fig is None:
            await _send_telegram(
                chat_id,
                caption + "\n\nДля визуализации «таблица» PNG не строится. Данные:\n" + _text_preview_table(res),
            )
        else:
            png = _fig_to_static_bytes(fig, "png")
            if png:
                await _send_telegram_photo(chat_id, png, caption)
            else:
                await _send_telegram(
                    chat_id,
                    caption + "\n\nНе удалось сгенерировать PNG (нужен kaleido). Данные:\n" + _text_preview_table(res),
                )
    elif out_fmt == "pdf":
        if fig is None:
            await _send_telegram(
                chat_id,
                caption + "\n\nДля таблицы PDF с графиком не строится. Данные:\n" + _text_preview_table(res),
            )
        else:
            pdf = _report_telegram_pdf_bytes(fig, report, res)
            if pdf:
                await _send_telegram_document(chat_id, "report.pdf", pdf, "application/pdf", caption)
            else:
                await _send_telegram(
                    chat_id,
                    caption + "\n\nНе удалось сгенерировать PDF. Данные:\n" + _text_preview_table(res),
                )
    elif out_fmt == "csv":
        raw = _df_to_csv_bytes(df)
        await _send_telegram_document(chat_id, "report.csv", raw, "text/csv", caption)
    elif out_fmt == "xlsx":
        try:
            xb = _df_to_xlsx_bytes(df)
            await _send_telegram_document(
                chat_id,
                "report.xlsx",
                xb,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                caption,
            )
        except Exception as e:
            log.warning("XLSX export failed: %s", e)
            await _send_telegram(chat_id, caption + "\n\nНе удалось сформировать XLSX. Данные:\n" + _text_preview_table(res))

    db_mark_report_sent(report_id)


def _sync_schedule_jobs() -> None:
    global _scheduler
    if _scheduler is None:
        return
    reports = db_list_reports(enabled_only=True)
    current_ids = {int(r["id"]) for r in reports}

    for report_id in list(_registered_jobs):
        if report_id not in current_ids:
            job_id = _registered_jobs.pop(report_id)
            try:
                _scheduler.remove_job(job_id)
            except Exception:
                pass

    for rpt in reports:
        report_id = int(rpt["id"])
        cron_expr = report_to_cron(
            str(rpt.get("freq") or "weekly"),
            rpt.get("day"),
            str(rpt.get("time") or "09:00"),
        )
        parts = cron_expr.split()
        if len(parts) != 5:
            continue
        minute, hour, day, month, day_of_week = parts
        job = _scheduler.add_job(
            _run_scheduled_report,
            trigger=CronTrigger(
                minute=minute,
                hour=hour,
                day=day,
                month=month,
                day_of_week=day_of_week,
                timezone=MSK_TZ,
            ),
            args=[report_id],
            id=f"report_{report_id}",
            replace_existing=True,
            misfire_grace_time=300,
        )
        _registered_jobs[report_id] = str(job.id)


@asynccontextmanager
async def lifespan(_: FastAPI):
    global _scheduler
    _scheduler = AsyncIOScheduler(timezone=MSK_TZ)
    _scheduler.add_job(
        _sync_schedule_jobs,
        trigger=CronTrigger(second=0, timezone=MSK_TZ),
        id="sync_report_jobs",
        replace_existing=True,
    )
    _scheduler.start()
    _sync_schedule_jobs()
    try:
        yield
    finally:
        if _scheduler:
            _scheduler.shutdown(wait=False)
            _scheduler = None


app = FastAPI(title="Drivee NL→SQL", version="0.2.0", lifespan=lifespan)
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
            "stats_template": res.stats_template,
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
    cols = [str(c) for c in df.columns]
    x = cols[0]
    y = cols[1] if len(cols) > 1 else x

    if len(cols) >= 3 and chart_type in ("bar", "line", "area"):
        hue = cols[1]
        y_agg = cols[-1]
        d = df.sort_values(by=[x, hue])
        title = f"{y_agg} по {x} · {hue}"
        if chart_type == "line":
            return px.line(d, x=x, y=y_agg, color=hue, markers=True, title=title)
        if chart_type == "area":
            return px.bar(d, x=x, y=y_agg, color=hue, barmode="group", title=title)
        return px.bar(d, x=x, y=y_agg, color=hue, barmode="group", title=title)

    if chart_type == "line":
        return px.line(df.sort_values(by=x), x=x, y=y, markers=True,
                       title=f"{y} по {x}")
    if chart_type == "pie":
        if len(cols) >= 3:
            d = df.copy()
            lab_col = "_pie_lbl"
            d[lab_col] = d[cols[0]].astype(str) + " · " + d[cols[1]].astype(str)
            return px.pie(d, names=lab_col, values=cols[-1], title=f"Распределение {cols[-1]}")
        return px.pie(df, names=x, values=y, title=f"Распределение {y}")
    if chart_type == "kpi":
        return px.bar(df, x=x, y=y, title=f"{y}")
    if chart_type == "table":
        return None
    return px.bar(df, x=x, y=y, title=f"{y} по {x}")


def _parse_telegram_user_id(raw) -> int | None:
    if raw is None or (isinstance(raw, str) and not str(raw).strip()):
        return None
    try:
        v = int(raw)
    except (TypeError, ValueError):
        return None
    return v if v > 0 else None


@app.get("/reports")
def get_reports():
    reports = db_list_reports(enabled_only=False)
    return {"total": len(reports), "reports": reports}


@app.post("/reports")
def create_report(payload: dict = Body(...)):
    required = ("name", "query")
    missing = [f for f in required if not str(payload.get(f, "")).strip()]
    if missing:
        raise HTTPException(400, f"Отсутствуют поля: {missing}")

    report_id = db_add_report(
        name=str(payload["name"]).strip(),
        query=str(payload["query"]).strip(),
        sql=(str(payload.get("sql")).strip() if payload.get("sql") else None),
        chart_type=str(payload.get("chart_type", "auto")).strip(),
        schedule_enabled=1 if payload.get("schedule_enabled", True) else 0,
        freq=str(payload.get("freq", "weekly")).strip(),
        day=(str(payload.get("day")).strip() if payload.get("day") else None),
        time=str(payload.get("time", "09:00")).strip(),
        channel=str(payload.get("channel", "telegram")).strip(),
        telegram_user_id=_parse_telegram_user_id(payload.get("telegram_user_id")),
        email=str(payload.get("email", "")).strip(),
        format=str(payload.get("format", "pdf")).strip(),
    )
    _sync_schedule_jobs()
    return {"ok": True, "report": db_get_report(report_id)}


@app.patch("/reports/{report_id}")
def patch_report(report_id: int, payload: dict = Body(...)):
    if not db_get_report(report_id):
        raise HTTPException(404, "Отчёт не найден")
    data = dict(payload)
    if "telegram_user_id" in data:
        data["telegram_user_id"] = _parse_telegram_user_id(data.get("telegram_user_id"))
    db_update_report(report_id, **data)
    _sync_schedule_jobs()
    return {"ok": True, "report": db_get_report(report_id)}


@app.delete("/reports/{report_id}")
def remove_report(report_id: int):
    if not db_get_report(report_id):
        raise HTTPException(404, "Отчёт не найден")
    db_delete_report(report_id)
    _sync_schedule_jobs()
    return {"ok": True}


@app.get("/reports/schedule")
def get_reports_schedule():
    reports = db_list_reports(enabled_only=False)
    schedules = [{
        "report_id": r["id"],
        "name": r["name"],
        "schedule_enabled": bool(r.get("schedule_enabled")),
        "freq": r.get("freq"),
        "day": r.get("day"),
        "time": r.get("time"),
        "channel": r.get("channel"),
        "telegram_user_id": r.get("telegram_user_id"),
        "email": r.get("email"),
        "last_sent_at": r.get("last_sent_at"),
    } for r in reports]
    return {"total": len(schedules), "schedules": schedules}


@app.patch("/reports/schedule/{report_id}")
def patch_report_schedule(report_id: int, payload: dict = Body(...)):
    report = db_get_report(report_id)
    if not report:
        raise HTTPException(404, "Отчёт не найден")
    allowed = {
        "schedule_enabled", "freq", "day", "time",
        "channel", "telegram_user_id", "email"
    }
    updates = {k: v for k, v in payload.items() if k in allowed}
    if not updates:
        raise HTTPException(400, "Нет полей для обновления расписания")
    db_update_report(report_id, **updates)
    _sync_schedule_jobs()
    return {"ok": True, "report": db_get_report(report_id)}


def _bind_telegram_user_and_sync(telegram_user_id: int) -> bool:
    existing_chat_id = db_get_chat_id(telegram_user_id)
    db_save_chat_id(
        telegram_user_id=telegram_user_id,
        chat_id=existing_chat_id or telegram_user_id,
    )
    for rpt in db_list_reports(enabled_only=False):
        if not rpt.get("telegram_user_id"):
            db_update_report(int(rpt["id"]), telegram_user_id=telegram_user_id)
    _sync_schedule_jobs()
    return existing_chat_id is not None


@app.get("/settings/delivery")
def get_delivery_settings():
    s = db_get_app_delivery_settings()
    return {"ok": True, **s}


@app.post("/settings/delivery")
def save_delivery_settings(payload: dict = Body(...)):
    telegram_user_id = _parse_telegram_user_id(payload.get("telegram_user_id"))
    if telegram_user_id is None:
        raise HTTPException(400, "Нужно числовое поле telegram_user_id")
    email = str(payload.get("email", "")).strip()
    channel = str(payload.get("channel", "both")).strip()
    if channel not in ("telegram", "email", "both"):
        channel = "both"
    db_save_app_delivery_settings(telegram_user_id, email, channel)
    bot_linked = _bind_telegram_user_and_sync(telegram_user_id)
    return {
        "ok": True,
        "telegram_user_id": telegram_user_id,
        "email": email,
        "channel": channel,
        "bot_linked": bot_linked,
    }


@app.post("/settings/telegram")
def save_telegram_settings(payload: dict = Body(...)):
    telegram_user_id = payload.get("telegram_user_id")
    if telegram_user_id is None or str(telegram_user_id).strip() == "":
        raise HTTPException(400, "Нужно поле telegram_user_id")
    try:
        telegram_user_id = int(telegram_user_id)
    except (TypeError, ValueError):
        raise HTTPException(400, "telegram_user_id должен быть числом")
    if telegram_user_id <= 0:
        raise HTTPException(400, "telegram_user_id должен быть положительным числом")

    cur = db_get_app_delivery_settings()
    ch = (cur.get("channel") or "both").strip()
    if ch not in ("telegram", "email", "both"):
        ch = "both"
    db_save_app_delivery_settings(telegram_user_id, cur.get("email") or "", ch)
    bot_linked = _bind_telegram_user_and_sync(telegram_user_id)
    return {
        "ok": True,
        "telegram_user_id": telegram_user_id,
        "bot_linked": bot_linked,
    }


@app.get("/settings/telegram/{telegram_user_id}")
def get_telegram_settings_status(telegram_user_id: int):
    chat_id = db_get_chat_id(telegram_user_id)
    last_at = db_last_dispatch_at(telegram_user_id)
    return {
        "telegram_user_id": telegram_user_id,
        "bot_linked": chat_id is not None,
        "last_dispatch_at": last_at,
    }


@app.post("/settings/telegram/test")
def test_telegram_delivery(payload: dict = Body(...)):
    raw = payload.get("telegram_user_id")
    if raw is None or str(raw).strip() == "":
        raise HTTPException(400, "Укажите telegram_user_id")
    try:
        telegram_user_id = int(raw)
    except (TypeError, ValueError):
        raise HTTPException(400, "telegram_user_id должен быть числом")
    if telegram_user_id <= 0:
        raise HTTPException(400, "telegram_user_id должен быть положительным числом")

    if not BOT_TOKEN:
        raise HTTPException(503, "TELEGRAM_BOT_TOKEN не задан на сервере")

    chat_id = db_get_chat_id(telegram_user_id)
    if chat_id is None:
        chat_id = int(telegram_user_id)

    try:
        r = httpx.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            json={
                "chat_id": chat_id,
                "text": "✅ Тест «Умный Аналитик»: доставка в этот чат работает.",
            },
            timeout=20,
        )
        r.raise_for_status()
    except httpx.HTTPStatusError as e:
        raise HTTPException(502, f"Telegram API: {e.response.text[:500]}") from e
    except Exception as e:
        raise HTTPException(502, f"Не удалось отправить: {e}") from e
    log.info("Telegram test OK chat_id=%s user_id=%s", chat_id, telegram_user_id)
    return {"ok": True, "chat_id": chat_id}


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


@app.delete("/semantic-layer/synonym")
def remove_synonym(term: str, synonym: str):
    if not term or not synonym:
        raise HTTPException(400, "Нужны query-параметры term и synonym")
    deleted = semantic.remove_synonym(term, synonym, actor="user")
    return {"ok": deleted, "term": term, "synonym": synonym}


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
