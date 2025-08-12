"""
Integrated WhatsApp automation: save → report → search → schedule

Functions
- save_chat_log(group_name, df, period)
- generate_morning_briefing(group_name, date)
- generate_weekly_report(group_name, week_start)
- generate_monthly_report(group_name, month)
- search_chat(group_name, start_date, end_date, keyword)

Storage
- Per-group daily SQLite at data/whatsapp_logs/{group}_{YYYYMMDD}_{period}.sqlite (table chat_log)
- KPI summary table (kpi_summary) at data/whatsapp_kpi.sqlite
"""
from __future__ import annotations

import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import List, Optional

import pandas as pd

try:
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfgen import canvas
    REPORTLAB_AVAILABLE = True
except Exception:
    REPORTLAB_AVAILABLE = False

DATA_ROOT = Path("data")
LOG_DB_DIR = DATA_ROOT / "whatsapp_logs"
REPORT_DIR = DATA_ROOT / "reports"
KPI_DB_PATH = DATA_ROOT / "whatsapp_kpi.sqlite"

# ENV override
DATA_ROOT = Path(os.getenv("HVDC_DATA_DIR", str(DATA_ROOT)))
LOG_DB_DIR = Path(os.getenv("HVDC_WHATSAPP_LOG_DIR", str(LOG_DB_DIR)))


def _ensure_dirs() -> None:
    LOG_DB_DIR.mkdir(parents=True, exist_ok=True)
    (REPORT_DIR / "morning").mkdir(parents=True, exist_ok=True)
    (REPORT_DIR / "weekly").mkdir(parents=True, exist_ok=True)
    (REPORT_DIR / "monthly").mkdir(parents=True, exist_ok=True)
    DATA_ROOT.mkdir(parents=True, exist_ok=True)


def _sanitize_group(group_name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", group_name.strip())[:64]


def _open_conn(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS chat_log (
            msg_id TEXT,
            date TEXT,
            time TEXT,
            sender TEXT,
            sender_role TEXT,
            message TEXT,
            tags TEXT,
            sla_breach INTEGER,
            attachments TEXT
        )
        """
    )
    return conn


def _ensure_kpi_db() -> None:
    with sqlite3.connect(str(KPI_DB_PATH)) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS kpi_summary (
                period TEXT,
                start_date TEXT,
                end_date TEXT,
                group_name TEXT,
                total_messages INTEGER,
                urgent_count INTEGER,
                sla_breach_count INTEGER,
                top_keywords TEXT,
                top_senders TEXT
            )
            """
        )


def _df_to_rows(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    list_cols = [
        c for c in out.columns if out[c].apply(lambda v: isinstance(v, (list, tuple))).any()
    ]
    for c in list_cols:
        out[c] = out[c].apply(lambda v: ", ".join(map(str, v)) if isinstance(v, (list, tuple)) else v)
    for col in ["sla_breach"]:
        if col in out.columns:
            out[col] = out[col].fillna(0).astype(int)
    # Ensure all required columns exist
    for required in [
        "msg_id",
        "date",
        "time",
        "sender",
        "sender_role",
        "message",
        "tags",
        "sla_breach",
        "attachments",
    ]:
        if required not in out.columns:
            out[required] = "" if required != "sla_breach" else 0
    # Auto msg_id when missing
    def gen_id(row) -> str:
        base = f"{row['date']} {row['time']}|{row['sender']}|{row['message'][:64]}"
        return str(abs(hash(base)))

    out["msg_id"] = out["msg_id"].apply(lambda v: v if v else None)
    out.loc[out["msg_id"].isna(), "msg_id"] = out[out["msg_id"].isna()].apply(gen_id, axis=1)
    return out[[
        "msg_id",
        "date",
        "time",
        "sender",
        "sender_role",
        "message",
        "tags",
        "sla_breach",
        "attachments",
    ]]


def save_chat_log(group_name: str, df: pd.DataFrame, period: str) -> Path:
    _ensure_dirs()
    safe_group = _sanitize_group(group_name)
    day_str = datetime.now().strftime("%Y%m%d")
    db_path = LOG_DB_DIR / f"{safe_group}_{day_str}_{period}.sqlite"
    rows = _df_to_rows(df)
    with _open_conn(db_path) as conn:
        rows.to_sql("chat_log", conn, if_exists="append", index=False)
    # Also export CSV for convenience
    csv_path = db_path.with_suffix(".csv")
    rows.to_csv(csv_path, index=False, encoding="utf-8")
    return db_path


def _load_range(group_name: str, start: date, end: date) -> pd.DataFrame:
    safe_group = _sanitize_group(group_name)
    frames: List[pd.DataFrame] = []
    for p in LOG_DB_DIR.glob(f"{safe_group}_*.sqlite"):
        # Parse date from filename pattern group_YYYYMMDD_period.sqlite
        try:
            parts = p.stem.split("_")
            ymd = next(x for x in parts if re.fullmatch(r"\d{8}", x))
            file_date = datetime.strptime(ymd, "%Y%m%d").date()
        except Exception:
            continue
        if start <= file_date <= end:
            with sqlite3.connect(str(p)) as conn:
                try:
                    df = pd.read_sql_query("SELECT * FROM chat_log", conn)
                    frames.append(df)
                except Exception:
                    pass
    if not frames:
        return pd.DataFrame(columns=[
            "msg_id","date","time","sender","sender_role","message","tags","sla_breach","attachments"
        ])
    df_all = pd.concat(frames, ignore_index=True)
    return df_all


def generate_morning_briefing(group_name: str, day: Optional[date] = None) -> Path:
    _ensure_dirs()
    if day is None:
        day = datetime.now().date()
    yesterday = day - timedelta(days=1)
    # Window: from yesterday 00:00 to day 07:00
    df = _load_range(group_name, yesterday, day)
    # Filter up to 07:00 of today
    def in_window(r) -> bool:
        try:
            d = datetime.strptime(str(r["date"]), "%Y-%m-%d").date()
            if d < day:
                return True
            if d > day:
                return False
            t = datetime.strptime(str(r["time"]), "%H:%M").time()
            return t <= time(7, 0)
        except Exception:
            return True

    if not df.empty:
        df = df[df.apply(in_window, axis=1)]

    report_file = REPORT_DIR / "morning" / f"{_sanitize_group(group_name)}_{day}.md"
    with report_file.open("w", encoding="utf-8") as f:
        f.write(f"# {day} 아침 회의 보고서 ({group_name})\n\n")
        # 1. 전날 주요 업무
        f.write("## 1. 전날 주요 업무\n")
        prev = df[df["date"].astype(str) == str(yesterday)] if not df.empty else pd.DataFrame()
        for m in prev.get("message", []).tolist():
            f.write(f"- {m}\n")
        f.write("\n")
        # 2. 오늘 할 일
        f.write("## 2. 오늘 할 일\n")
        action = df[df.get("tags", "").astype(str).str.contains("ACTION", na=False)] if not df.empty else pd.DataFrame()
        for m in action.get("message", []).tolist():
            f.write(f"- {m}\n")
        f.write("\n")
        # 3. 중요 이슈
        f.write("## 3. 중요 이슈\n")
        important = df[df.get("tags", "").astype(str).str.contains("IMPORTANT", na=False)] if not df.empty else pd.DataFrame()
        for m in important.get("message", []).tolist():
            f.write(f"- {m}\n")
        f.write("\n")
        # 4. 급한 일
        f.write("## 4. 급한 일 (URGENT)\n")
        urgent = df[df.get("tags", "").astype(str).str.contains("URGENT", na=False)] if not df.empty else pd.DataFrame()
        for m in urgent.get("message", []).tolist():
            f.write(f"- {m}\n")
        f.write("\n")
        # 5. 놓친 일
        f.write("## 5. 놓친 일 / 미처리 건\n")
        missed = df[df.get("sla_breach", 0).astype(int) == 1] if not df.empty else pd.DataFrame()
        for m in missed.get("message", []).tolist():
            f.write(f"- {m}\n")
    return report_file


def _aggregate_kpi(df: pd.DataFrame) -> dict:
    if df.empty:
        return {
            "total_messages": 0,
            "urgent_count": 0,
            "sla_breach_count": 0,
            "top_keywords": "",
            "top_senders": "",
        }
    urgent_count = df["tags"].astype(str).str.contains("URGENT", na=False).sum()
    sla_breach_count = df.get("sla_breach", 0).astype(int).sum()
    # crude keywords split
    words = (
        df["message"].astype(str).str.lower().str.findall(r"[a-zA-Z0-9_]{3,}").explode().dropna()
    )
    top_keywords = ", ".join(words.value_counts().head(5).index.tolist())
    top_senders = ", ".join(df["sender"].astype(str).value_counts().head(5).index.tolist())
    return {
        "total_messages": int(len(df)),
        "urgent_count": int(urgent_count),
        "sla_breach_count": int(sla_breach_count),
        "top_keywords": top_keywords,
        "top_senders": top_senders,
    }


def _write_kpi(period: str, start_d: date, end_d: date, group_name: str, kpi: dict) -> None:
    _ensure_kpi_db()
    with sqlite3.connect(str(KPI_DB_PATH)) as conn:
        conn.execute(
            """
            INSERT INTO kpi_summary(period, start_date, end_date, group_name,
                                    total_messages, urgent_count, sla_breach_count,
                                    top_keywords, top_senders)
            VALUES(?,?,?,?,?,?,?,?,?)
            """,
            (
                period,
                start_d.isoformat(),
                end_d.isoformat(),
                group_name,
                kpi["total_messages"],
                kpi["urgent_count"],
                kpi["sla_breach_count"],
                kpi["top_keywords"],
                kpi["top_senders"],
            ),
        )


def _write_pdf(path: Path, title: str, lines: List[str]) -> Path:
    if not REPORTLAB_AVAILABLE:
        # Fallback to .md
        md_path = path.with_suffix(".md")
        with md_path.open("w", encoding="utf-8") as f:
            f.write(f"# {title}\n\n")
            for ln in lines:
                f.write(ln + "\n")
        return md_path
    c = canvas.Canvas(str(path), pagesize=A4)
    width, height = A4
    y = height - 40
    c.setFont("Helvetica-Bold", 14)
    c.drawString(40, y, title)
    y -= 24
    c.setFont("Helvetica", 10)
    for ln in lines:
        if y < 40:
            c.showPage()
            y = height - 40
            c.setFont("Helvetica", 10)
        c.drawString(40, y, ln[:110])
        y -= 14
    c.showPage()
    c.save()
    return path


def generate_weekly_report(group_name: str, week_start: Optional[date] = None) -> Path:
    _ensure_dirs()
    if week_start is None:
        today = datetime.now().date()
        week_start = today - timedelta(days=today.weekday())
    week_end = week_start + timedelta(days=6)
    df = _load_range(group_name, week_start, week_end)
    kpi = _aggregate_kpi(df)
    _write_kpi("weekly", week_start, week_end, group_name, kpi)
    lines = [
        f"기간: {week_start} ~ {week_end}",
        f"총 메시지 수: {kpi['total_messages']}",
        f"URGENT 건수: {kpi['urgent_count']}",
        f"SLA 위반 건수: {kpi['sla_breach_count']}",
        f"상위 키워드: {kpi['top_keywords']}",
        f"상위 발신자: {kpi['top_senders']}",
    ]
    out = REPORT_DIR / "weekly" / f"{_sanitize_group(group_name)}_{week_start}_weekly.pdf"
    return _write_pdf(out, f"{group_name} 주간 보고서", lines)


def generate_monthly_report(group_name: str, month_start: Optional[date] = None) -> Path:
    _ensure_dirs()
    if month_start is None:
        today = datetime.now().date()
        month_start = today.replace(day=1)
    # compute month end
    month_end = (month_start + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    df = _load_range(group_name, month_start, month_end)
    kpi = _aggregate_kpi(df)
    _write_kpi("monthly", month_start, month_end, group_name, kpi)
    lines = [
        f"기간: {month_start} ~ {month_end}",
        f"총 메시지 수: {kpi['total_messages']}",
        f"URGENT 건수: {kpi['urgent_count']}",
        f"SLA 위반 건수: {kpi['sla_breach_count']}",
        f"상위 키워드: {kpi['top_keywords']}",
        f"상위 발신자: {kpi['top_senders']}",
    ]
    out = REPORT_DIR / "monthly" / f"{_sanitize_group(group_name)}_{month_start.strftime('%Y%m')}_monthly.pdf"
    return _write_pdf(out, f"{group_name} 월간 보고서", lines)


def search_chat(group_name: str, start_date: str, end_date: str, keyword: str) -> pd.DataFrame:
    start_d = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_d = datetime.strptime(end_date, "%Y-%m-%d").date()
    df = _load_range(group_name, start_d, end_d)
    if df.empty:
        return df
    mask = df["message"].astype(str).str.contains(re.escape(keyword), case=False, na=False)
    return df.loc[mask, ["date", "time", "sender", "message", "attachments"]].copy()


def _example_dataframe() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "msg_id": "",
            "date": datetime.now().strftime("%Y-%m-%d"),
            "time": datetime.now().strftime("%H:%M"),
            "sender": "OPS",
            "sender_role": "Operator",
            "message": "Crane delay resolved; resume 08:00",
            "tags": "ACTION",
            "sla_breach": 0,
            "attachments": "",
        },
        {
            "msg_id": "",
            "date": datetime.now().strftime("%Y-%m-%d"),
            "time": (datetime.now() - timedelta(minutes=30)).strftime("%H:%M"),
            "sender": "Port",
            "sender_role": "OPS",
            "message": "High wind warning",
            "tags": "URGENT",
            "sla_breach": 1,
            "attachments": "",
        },
    ])


def _cli():
    import argparse

    parser = argparse.ArgumentParser(description="WhatsApp automation CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_save = sub.add_parser("save", help="Save sample chat log (demo)")
    p_save.add_argument("group", help="Group name")
    p_save.add_argument("period", choices=["daily", "weekly", "monthly", "adhoc"], help="Period tag")

    p_morning = sub.add_parser("morning", help="Generate morning briefing")
    p_morning.add_argument("group", help="Group name")
    p_morning.add_argument("--date", help="YYYY-MM-DD", default=None)

    p_week = sub.add_parser("weekly", help="Generate weekly report")
    p_week.add_argument("group", help="Group name")
    p_week.add_argument("--start", help="Week start YYYY-MM-DD", default=None)

    p_month = sub.add_parser("monthly", help="Generate monthly report")
    p_month.add_argument("group", help="Group name")
    p_month.add_argument("--month", help="Month start YYYY-MM-01", default=None)

    p_search = sub.add_parser("search", help="Search chat by date-range and keyword")
    p_search.add_argument("group", help="Group name")
    p_search.add_argument("start", help="YYYY-MM-DD")
    p_search.add_argument("end", help="YYYY-MM-DD")
    p_search.add_argument("keyword", help="Keyword")

    p_sched = sub.add_parser("schedule", help="Run scheduler for periodic reports")
    p_sched.add_argument("group", help="Group name")

    args = parser.parse_args()
    _ensure_dirs()

    if args.cmd == "save":
        df = _example_dataframe()
        path = save_chat_log(args.group, df, args.period)
        print(f"Saved: {path}")
    elif args.cmd == "morning":
        d = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else None
        path = generate_morning_briefing(args.group, d)
        print(f"Generated: {path}")
    elif args.cmd == "weekly":
        ws = datetime.strptime(args.start, "%Y-%m-%d").date() if args.start else None
        path = generate_weekly_report(args.group, ws)
        print(f"Generated: {path}")
    elif args.cmd == "monthly":
        ms = datetime.strptime(args.month, "%Y-%m-%d").date() if args.month else None
        path = generate_monthly_report(args.group, ms)
        print(f"Generated: {path}")
    elif args.cmd == "search":
        res = search_chat(args.group, args.start, args.end, args.keyword)
        print(res.to_string(index=False))
    elif args.cmd == "schedule":
        try:
            import schedule
        except Exception:
            print("Install 'schedule' to use scheduler: pip install schedule")
            raise SystemExit(1)
        group = args.group
        # Daily 07:00 morning
        schedule.every().day.at("07:00").do(lambda: generate_morning_briefing(group))
        # Weekly Monday 08:00
        schedule.every().monday.at("08:00").do(lambda: generate_weekly_report(group))
        # Monthly 1st 09:00 (approx: check daily)
        def monthly_job():
            today = datetime.now().date()
            if today.day == 1:
                generate_monthly_report(group)
        schedule.every().day.at("09:00").do(monthly_job)
        print("Scheduler started. Press Ctrl+C to stop.")
        while True:
            schedule.run_pending()
            import time as _t
            _t.sleep(1)


if __name__ == "__main__":
    _cli()


