import os
import json
import time
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh  # pip install streamlit-autorefresh

# -----------------------------
# Config: 로컬 경로(환경변수 우선)
# -----------------------------
DATA_DIR = Path(os.getenv("HVDC_DATA_DIR", r"C:\hvdc\data"))
QUEUE_FILE = Path(os.getenv("HVDC_RETRY_QUEUE", str(DATA_DIR / "retry_queue.jsonl")))
LOG_DIR = Path(os.getenv("HVDC_WHATSAPP_LOG_DIR", str(DATA_DIR / "whatsapp_logs")))
KPI_CSV = Path(os.getenv("HVDC_KPI_CSV", str(DATA_DIR / "kpi_report.csv")))
KPI_SQLITE = Path(os.getenv("HVDC_KPI_SQLITE", str(DATA_DIR / "logs.sqlite")))

REFRESH_SECS = int(os.getenv("HVDC_DASHBOARD_REFRESH", "10"))  # 자동 새로고침 간격(초)

st.set_page_config(page_title="HVDC Local Dashboard", layout="wide")

# 자동 새로고침(클린하게 재실행)
st_autorefresh(interval=REFRESH_SECS * 1000, key="auto_refresh")

st.title("⚡ HVDC Local Dashboard")
st.caption(f"DATA_DIR: {DATA_DIR}")

# --------------------------------
# Helpers
# --------------------------------
def _read_jsonl(path: Path, nrows: int | None = None) -> pd.DataFrame:
    """JSON Lines 파일 -> DataFrame (lines=True)"""
    if not path.exists():
        return pd.DataFrame()
    # pandas.read_json(lines=True) 사용 권장. :contentReference[oaicite:2]{index=2}
    try:
        return pd.read_json(path, lines=True) if nrows is None else pd.read_json(path, lines=True, nrows=nrows)
    except ValueError:
        # 포맷 오류 시 안전하게 빈 DF
        return pd.DataFrame()

def _read_latest_appendlogs(log_dir: Path, limit: int = 50) -> pd.DataFrame:
    if not log_dir.exists():
        return pd.DataFrame()
    rows = []
    for p in sorted(log_dir.glob("*.json"), reverse=True)[:limit]:
        try:
            with open(p, "r", encoding="utf-8") as f:
                obj = json.load(f)
            obj["_file"] = str(p)
            rows.append(obj)
        except Exception:
            continue
    return pd.DataFrame(rows)

# 캐시된 로더들(데이터 반환 캐싱). :contentReference[oaicite:3]{index=3}
@st.cache_data(ttl=5)  # 소규모 파일은 5초 캐시
def load_queue_df() -> pd.DataFrame:
    return _read_jsonl(QUEUE_FILE)

@st.cache_data(ttl=5)
def load_logs_df(limit: int = 100) -> pd.DataFrame:
    return _read_latest_appendlogs(LOG_DIR, limit=limit)

@st.cache_data(ttl=10)
def load_kpi_csv() -> pd.DataFrame:
    if KPI_CSV.exists():
        try:
            return pd.read_csv(KPI_CSV)
        except Exception:
            pass
    return pd.DataFrame()

@st.cache_data(ttl=10)
def load_kpi_sqlite() -> pd.DataFrame:
    if not KPI_SQLITE.exists():
        return pd.DataFrame()
    # sqlite3.connect로 조회(표준 라이브러리). :contentReference[oaicite:4]{index=4}
    con = sqlite3.connect(str(KPI_SQLITE))
    try:
        # 사용 중인 KPI 뷰/테이블 이름에 맞춰 조정하세요.
        # 예: v_kpi_daily(date, group_name, logs, sla_breaches)
        return pd.read_sql_query("SELECT * FROM v_kpi_daily", con)
    except Exception:
        try:
            # 폴백: 대략적인 구조 추정
            return pd.read_sql_query("SELECT * FROM kpi_daily", con)
        except Exception:
            return pd.DataFrame()
    finally:
        con.close()

# -----------------------------
# Layout
# -----------------------------
colA, colB, colC = st.columns(3)

# Queue KPIs
qdf = load_queue_df()
queued = int((qdf["status"] == "queued").sum()) if not qdf.empty else 0
retrying = int((qdf["status"] == "retrying").sum()) if not qdf.empty else 0
dead = int((qdf["status"] == "deadletter").sum()) if not qdf.empty else 0
done = int((qdf["status"] == "done").sum()) if not qdf.empty else 0

colA.metric("Queued", queued)
colB.metric("Retrying", retrying)
colC.metric("Deadletter", dead)

st.divider()

# 좌: 최신 appendLog / 우: KPI
left, right = st.columns([1.2, 1])

with left:
    st.subheader("📝 Latest appendLog files")
    ldf = load_logs_df(limit=200)
    if ldf.empty:
        st.info(f"No JSON files under {LOG_DIR}")
    else:
        # 주요 열 정리
        cols = [c for c in ["date_gst","group_name","summary","top_keywords","sla_breaches","_file"] if c in ldf.columns]
        st.dataframe(ldf[cols], use_container_width=True, hide_index=True)
        st.caption(f"{len(ldf)} rows · dir={LOG_DIR}")

with right:
    st.subheader("📈 KPI Overview")
    k1 = load_kpi_csv()
    k2 = load_kpi_sqlite()

    tab1, tab2 = st.tabs(["KPI (CSV)", "KPI (SQLite)"])
    with tab1:
        if k1.empty:
            st.warning(f"No CSV at {KPI_CSV}")
        else:
            st.dataframe(k1, use_container_width=True, hide_index=True)
            # CSV 다운로드 버튼
            st.download_button("Download KPI CSV", data=k1.to_csv(index=False).encode("utf-8"),
                               file_name="kpi_export.csv", mime="text/csv")

    with tab2:
        if k2.empty:
            st.warning(f"No SQLite KPI at {KPI_SQLITE}")
        else:
            st.dataframe(k2, use_container_width=True, hide_index=True)

st.divider()

# Queue raw table(최근 N행)
st.subheader("🔁 Retry Queue (recent)")
if qdf.empty:
    st.info(f"No queue file at {QUEUE_FILE}")
else:
    # 최신순으로 보여주기(파일을 역순 스캔하지 못했을 때 대비)
    view = qdf.copy()
    # 시간 문자열 컬럼이 있으면 정렬
    for tcol in ["next_try_at_gst","enqueued_at_gst"]:
        if tcol in view.columns:
            # 정렬 실패해도 무시
            try:
                view[tcol] = pd.to_datetime(view[tcol], errors="coerce", format="%Y-%m-%d %H:%M")
            except Exception:
                pass
    # 보여줄 열 추림
    show_cols = [c for c in ["type","status","attempt","max_attempts","next_try_at_gst","last_error","idempotency_key"] if c in view.columns]
    view = view.sort_values(by=[c for c in ["next_try_at_gst","enqueued_at_gst"] if c in view.columns], ascending=False)
    st.dataframe(view[show_cols], use_container_width=True, hide_index=True)

# Footer
st.caption(f"Auto-refresh: every {REFRESH_SECS}s · Now: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
