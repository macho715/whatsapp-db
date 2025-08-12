from fastapi import FastAPI, Header, HTTPException, Request, Query, APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime, timezone, timedelta
import base64, hmac, hashlib, os, csv, sqlite3, json
from pathlib import Path
import subprocess
import sys
import os
from fastapi.openapi.utils import get_openapi
import threading
import time
import psutil
import json, re
import io
from fastapi import Response
from typing import Optional
from hvdc_logs.pipeline_sequence import run_pipeline_sequence

# --- 설정 ---
API_KEY = os.getenv("API_KEY", "")  # 선택
HMAC_SECRET = os.getenv("HMAC_SECRET", "")  # 선택
# 우선순위: HVDC_DATA_DIR > DATA_DIR > ./data
DATA_DIR = Path(os.getenv("HVDC_DATA_DIR") or os.getenv("DATA_DIR", "data"))
CSV_PATH = DATA_DIR / "logs.csv"
SQLITE_PATH = DATA_DIR / "logs.sqlite"
# WhatsApp JSON 저장 루트 (선택)
WHATSAPP_LOG_DIR = Path(os.getenv("HVDC_WHATSAPP_LOG_DIR", r"C:\hvdc\data\whatsapp_logs"))

DUCKDB_PATH = Path(os.getenv("DUCKDB_PATH", "hvdc_logs/duckdb/hvdc.duckdb"))
DUCKDB_ENABLED = DUCKDB_PATH.exists()

# HVDC Pipeline paths
HVDC_BASE = Path("hvdc_logs")
HVDC_PIPELINE_SCRIPT = HVDC_BASE / "run_pipeline.py"
HVDC_TRANSFORM_SQL = HVDC_BASE / "transform.sql"

# --- Bronze 자동화 설정 ---
GST = timezone(timedelta(hours=4))  # Asia/Dubai
BRONZE_ROOT = Path("hvdc_logs/bronze")

# --- 파이프라인 디바운스 설정 ---
_last_run = 0
_lock = threading.Lock()
DEBOUNCE_SECS = 60
start_ts = time.time()

TZ = timezone.utc

# --- 보조: 보안 ---
def _require_api_key(x_api_key: Optional[str]):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

def _verify_hmac(raw_body: bytes, x_signature: Optional[str]):
    if not HMAC_SECRET:
        return
    if not x_signature:
        raise HTTPException(status_code=401, detail="Missing x-signature")
    mac = hmac.new(HMAC_SECRET.encode("utf-8"), raw_body, hashlib.sha256).digest()
    expected = base64.b64encode(mac).decode("ascii")
    if not hmac.compare_digest(expected, x_signature):
        raise HTTPException(status_code=401, detail="Invalid signature")

# --- 보조: 저장소 ---
def _ensure_storage():
    """Ensure data directories and files exist"""
    DATA_DIR.mkdir(exist_ok=True)
    CSV_PATH.parent.mkdir(exist_ok=True)
    SQLITE_PATH.parent.mkdir(exist_ok=True)
    
    # Create CSV if not exists
    if not CSV_PATH.exists():
        with CSV_PATH.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "date_gst", "group_name", "summary", "top_keywords",
                "sla_breaches", "attachments", "created_at", "request_id", "processed_status"
            ])
    
    # Create SQLite if not exists
    if not SQLITE_PATH.exists():
        conn = sqlite3.connect(str(SQLITE_PATH))
        conn.execute("""
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date_gst TEXT NOT NULL,
                group_name TEXT NOT NULL,
                summary TEXT NOT NULL,
                top_keywords TEXT,
                sla_breaches INTEGER DEFAULT 0,
                attachments TEXT,
                created_at TEXT NOT NULL,
                request_id TEXT UNIQUE,
                processed_status TEXT DEFAULT 'ok'
            )
        """)
        conn.commit()
        conn.close()

# --- Bronze 자동화 함수들 ---
def _mask_pii(text: str) -> str:
    """PII 마스킹 (전화번호, 이메일 등)"""
    if not text:
        return text
    # 전화번호/이메일 간단 마스킹 (정책에 맞게 보완 가능)
    text = re.sub(r'\b(\+?\d[\d\-\s]{6,}\d)\b', '****', text)
    text = re.sub(r'[\w\.-]+@[\w\.-]+\.\w+', '****', text)
    return text

def write_bronze_jsonl(item: dict) -> Path:
    """로그를 Bronze JSONL 파일에 자동 적재"""
    dt = datetime.strptime(item["date_gst"], "%Y-%m-%d %H:%M").replace(tzinfo=GST)
    y, m, d = dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%Y-%m-%d")
    # 파일명 안전 문자만 허용: 영문/숫자/._-  (나머지는 -로 치환)
    safe = re.sub(r'[^A-Za-z0-9._-]+', '-', item["group_name"].strip())
    group = re.sub(r'-{2,}', '-', safe).strip('-')
    outdir = BRONZE_ROOT / y / m
    outdir.mkdir(parents=True, exist_ok=True)
    fn = outdir / f"{d}_{group}.jsonl"
    
    payload = dict(item)
    payload["summary"] = _mask_pii(payload.get("summary", ""))
    
    with fn.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    
    return fn

# --- 파이프라인 디바운스 함수들 ---
def trigger_hvdc_pipeline_debounced():
    """60초 디바운스로 HVDC 파이프라인 자동 트리거"""
    global _last_run
    with _lock:
        now = time.time()
        if now - _last_run < DEBOUNCE_SECS:
            return
        _last_run = now
    
    threading.Thread(target=_run_pipeline, daemon=True).start()

def _run_pipeline():
    """백그라운드에서 HVDC 파이프라인 실행"""
    try:
        subprocess.run(
            ["python", "run_pipeline.py"], 
            check=True, 
            cwd=HVDC_BASE,
            capture_output=True,
            text=True
        )
        print("HVDC pipeline completed successfully")
    except Exception as e:
        print(f"HVDC pipeline error: {e}")

def _csv_append(row: List[str]):
    with CSV_PATH.open("a", newline="", encoding="utf-8") as f:
        csv.writer(f).writerow(row)

def _sqlite_insert(payload: dict):
    with sqlite3.connect(SQLITE_PATH) as conn:
        try:
            conn.execute("""
              INSERT INTO logs(date_gst, group_name, summary, top_keywords,
                               sla_breaches, attachments, created_at, request_id, processed_status)
              VALUES(?,?,?,?,?,?,?,?,?)
            """, (
                payload["date_gst"],
                payload["group_name"],
                payload["summary"],
                json.dumps(payload.get("top_keywords", []), ensure_ascii=False),
                int(payload.get("sla_breaches", 0)),
                json.dumps(payload.get("attachments", []), ensure_ascii=False),
                payload["created_at"],
                payload.get("request_id"),
                "ok"
            ))
            conn.commit()
            return True
        except sqlite3.IntegrityError as e:
            if "UNIQUE constraint failed: logs.request_id" in str(e):
                return False
            raise

def _sqlite_query(limit: int = 10, since: Optional[str] = None, group_name: Optional[str] = None):
    q = "SELECT date_gst, group_name, summary, top_keywords, sla_breaches, attachments, created_at, request_id, processed_status FROM logs WHERE 1=1"
    params = []
    if since:
        q += " AND created_at >= ?"
        params.append(since)
    if group_name:
        q += " AND group_name LIKE ?"
        params.append(f"%{group_name}%")
    q += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    with sqlite3.connect(SQLITE_PATH) as conn:
        rows = [dict(
            date_gst=r[0], group_name=r[1], summary=r[2],
            top_keywords=json.loads(r[3] or "[]"),
            sla_breaches=r[4],
            attachments=json.loads(r[5] or "[]"),
            created_at=r[6], request_id=r[7], processed_status=r[8]
        ) for r in conn.execute(q, params).fetchall()]
    return rows

# --- HVDC Pipeline Integration ---
def _run_hvdc_pipeline():
    """Run unified local pipeline (Bronze→Silver→transform)."""
    try:
        return run_pipeline_sequence()
    except Exception as e:
        return {"status": "error", "message": str(e)}

def _get_hvdc_status():
    """Get HVDC pipeline status and file information"""
    try:
        status = {
            "pipeline_script": HVDC_PIPELINE_SCRIPT.exists(),
            "transform_sql": HVDC_TRANSFORM_SQL.exists(),
            "bronze_data": [],
            "silver_data": [],
            "duckdb_file": DUCKDB_PATH.exists()
        }
        
        bronze_path = HVDC_BASE / "bronze" / "2025" / "08"
        if bronze_path.exists():
            status["bronze_data"] = [f.name for f in bronze_path.glob("*.jsonl")]
        
        silver_path = HVDC_BASE / "silver" / "logs"
        if silver_path.exists():
            status["silver_data"] = [f.name for f in silver_path.rglob("*.parquet")]
        
        return status
    except Exception as e:
        return {"error": str(e)}

# --- DuckDB KPI ---
def _kpi_from_duckdb(since: Optional[str], until: Optional[str], group_name: Optional[str]):
    import duckdb
    
    try:
        # Use absolute path to DuckDB file
        duckdb_abs_path = DUCKDB_PATH.absolute()
        conn = duckdb.connect(str(duckdb_abs_path))
        
        try:
            # Try to query the v_kpi_daily view first
            try:
                q = "SELECT * FROM v_kpi_daily WHERE 1=1"
                params = []
                if since: q += " AND date >= ?"; params.append(since.split(" ")[0])
                if until: q += " AND date <= ?"; params.append(until.split(" ")[0])
                if group_name: q += " AND group_name LIKE ?"; params.append(f"%{group_name}%")
                q += " ORDER BY date DESC, group_name"
                
                df = conn.execute(q, params).fetch_df()
                return {
                    "status": "ok",
                    "since": since or "",
                    "until": until or "",
                    "metrics": [
                        {
                            "date": str(r["date"]),
                            "group_name": r["group_name"],
                            "logs_count": int(r["logs_count"]),
                            "total_sla_breaches": int(r["total_sla_breaches"]),
                            "unique_keywords_count": int(r["unique_keywords_count"]),
                        }
                        for _, r in df.iterrows()
                    ]
                }
            except Exception as view_error:
                # If view fails, try direct Parquet query
                silver_path = HVDC_BASE / "silver" / "logs"
                if silver_path.exists():
                    q = f"""
                    SELECT 
                        date,
                        group_name,
                        count(*) AS logs_count,
                        sum(sla_breaches) AS total_sla_breaches,
                        count(DISTINCT top_keywords) AS unique_keywords_count
                    FROM read_parquet('{silver_path}/**/*.parquet')
                    WHERE 1=1
                    """
                    params = []
                    if since: q += " AND date >= ?"; params.append(since.split(" ")[0])
                    if until: q += " AND date <= ?"; params.append(until.split(" ")[0])
                    if group_name: q += " AND group_name LIKE ?"; params.append(f"%{group_name}%")
                    q += " GROUP BY 1, 2 ORDER BY 1 DESC, 2"
                    
                    df = conn.execute(q, params).fetch_df()
                    return {
                        "status": "ok",
                        "since": since or "",
                        "until": until or "",
                        "metrics": [
                            {
                                "date": str(r["date"]),
                                "group_name": r["group_name"],
                                "logs_count": int(r["logs_count"]),
                                "total_sla_breaches": int(r["total_sla_breaches"]),
                                "unique_keywords_count": int(r["unique_keywords_count"]),
                            }
                            for _, r in df.iterrows()
                        ]
                    }
                else:
                    raise view_error
        except Exception as e:
            return {
                "status": "error",
                "message": f"DuckDB KPI query failed: {str(e)}",
                "fallback": "Use /kpi endpoint for SQLite-based KPI"
            }
        finally:
            conn.close()
    except Exception as e:
        return {
            "status": "error",
            "message": f"DuckDB connection failed: {str(e)}",
            "fallback": "Use /kpi endpoint for SQLite-based KPI"
        }

# --- SQLite KPI ---
def _kpi_from_sqlite(since: Optional[str], until: Optional[str], group_name: Optional[str]):
    q = "SELECT substr(date_gst,1,10) AS date, group_name, COUNT(*) AS logs_count, SUM(COALESCE(sla_breaches,0)) AS total_sla_breaches FROM logs WHERE 1=1"
    params = []
    if since: q += " AND created_at >= ?"; params.append(since)
    if until: q += " AND created_at <= ?"; params.append(until)
    if group_name: q += " AND group_name LIKE ?"; params.append(f"%{group_name}%")
    q += " GROUP BY 1,2 ORDER BY 1 DESC, 2"
    with sqlite3.connect(SQLITE_PATH) as conn:
        rows = conn.execute(q, params).fetchall()
    return {
        "status": "ok",
        "since": since or "",
        "until": until or "",
        "metrics": [
            {
                "date": r[0],
                "group_name": r[1],
                "logs_count": int(r[2]),
                "total_sla_breaches": int(r[3] or 0),
                "unique_keywords_count": None
            } for r in rows
        ]
    }

# --- 스키마 ---
class AppendLogRequest(BaseModel):
    request_id: Optional[str] = Field(None, description="Idempotency key (UUID 권장)")
    date_gst: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$")
    group_name: str = Field(..., max_length=200)
    summary: str = Field(..., max_length=5000)
    top_keywords: Optional[List[str]] = Field(default_factory=list)
    sla_breaches: Optional[int] = Field(default=0, ge=0)
    attachments: Optional[List[str]] = Field(default_factory=list)
    signature: Optional[str] = None

app = FastAPI(title="HVDC WhatsApp → Local KPI Store (FastAPI + DuckDB Pipeline)",
              version="2.0.0",
              description="CSV/SQLite 저장 + 멱등성 + HMAC + KPI + DuckDB 연동 + HVDC Pipeline 통합")

# === Request access logging middleware ===
from plyer import notification  # type: ignore

LOG_FILE = "access_log.jsonl"

@app.middleware("http")
async def log_requests(request: Request, call_next):
    entry = {
        "timestamp": datetime.now().isoformat(),
        "client_ip": getattr(request.client, "host", "-"),
        "method": request.method,
        "url": str(request.url),
        "api_key": request.headers.get("x-api-key"),
    }
    os.makedirs(os.path.dirname(LOG_FILE) or ".", exist_ok=True)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass

    print(f"🔔 API Access: {entry['client_ip']} → {entry['url']}")

    try:
        notification.notify(
            title="HVDC API Access",
            message=f"{entry['client_ip']} → {entry['url']}",
            timeout=5,
        )
    except Exception as e:
        print(f"❌ 알림 실패: {e}")

    return await call_next(request)

@app.on_event("startup")
def _startup():
    _ensure_storage()

# --- 디버그: 실제 경로 확인 ---
router = APIRouter()

@router.get("/debug/paths")
def debug_paths():
    return {
        "cwd": os.getcwd(),
        "data_dir": str(DATA_DIR),
        "csv_path": str(CSV_PATH),
        "sqlite_path": str(SQLITE_PATH),
        "whatsapp_log_dir": str(WHATSAPP_LOG_DIR),
        "exists": {
            "data_dir": DATA_DIR.exists(),
            "csv": CSV_PATH.exists(),
            "sqlite": SQLITE_PATH.exists(),
            "whatsapp_log_dir": WHATSAPP_LOG_DIR.exists(),
        },
        "env": {
            "HVDC_DATA_DIR": os.getenv("HVDC_DATA_DIR"),
            "DATA_DIR": os.getenv("DATA_DIR"),
            "HVDC_WHATSAPP_LOG_DIR": os.getenv("HVDC_WHATSAPP_LOG_DIR"),
        },
    }

app.include_router(router)

@app.get("/health")
def health():
    return {
        "status": "ok",
        "store": {"csv": str(CSV_PATH), "sqlite": str(SQLITE_PATH)},
        "duckdb": {"enabled": DUCKDB_ENABLED, "path": str(DUCKDB_PATH)},
        "hvdc_pipeline": _get_hvdc_status()
    }

@app.get("/logs")
def get_recent_rows(
    limit: int = Query(10, ge=1, le=200),
    since: Optional[str] = None,
    group_name: Optional[str] = None,
    x_api_key: Optional[str] = Header(None, alias="X-API-Key")
):
    _require_api_key(x_api_key)
    rows = _sqlite_query(limit=limit, since=since, group_name=group_name)
    return {"status": "ok", "rows": rows, "timestamp": datetime.utcnow().isoformat()}

@app.post("/logs")
async def append_log(
    req: Request,
    payload: AppendLogRequest,
    x_api_key: Optional[str] = Header(None, alias="X-API-Key"),
    x_signature: Optional[str] = Header(None, alias="X-Signature")
):
    _require_api_key(x_api_key)
    raw = await req.body()
    _verify_hmac(raw, x_signature)

    body = payload.dict()
    now_iso = datetime.utcnow().isoformat(timespec="seconds")
    body["created_at"] = now_iso

    _csv_append([
        body["date_gst"], body["group_name"], body["summary"],
        json.dumps(body.get("top_keywords", []), ensure_ascii=False),
        int(body.get("sla_breaches", 0)),
        json.dumps(body.get("attachments", []), ensure_ascii=False),
        body["created_at"], body.get("request_id"), "ok"
    ])

    inserted = _sqlite_insert(body)
    if not inserted:
        return JSONResponse(status_code=409, content={
            "status": "error",
            "message": "Duplicate request_id"
        })

    # Bronze JSONL 자동 적재
    try:
        bronze_file = write_bronze_jsonl(body)
        bronze_status = f"Bronze: {bronze_file.name}"
    except Exception as e:
        bronze_status = f"Bronze failed: {str(e)}"

    # HVDC 파이프라인 자동 트리거 (디바운스)
    trigger_hvdc_pipeline_debounced()

    return {
        "status": "ok",
        "idempotency_key": body.get("request_id", ""),
        "attempt": 1,
        "priority": "FYI",
        "sla_breach": int(body.get("sla_breaches", 0)),
        "message": "Stored in local CSV/SQLite + Bronze JSONL",
        "bronze_file": bronze_status,
        "pipeline_triggered": True
    }

@app.get("/kpi")
def get_kpi(
    since: Optional[str] = None,
    until: Optional[str] = None,
    group_name: Optional[str] = None,
    x_api_key: Optional[str] = Header(None, alias="X-API-Key")
):
    _require_api_key(x_api_key)
    if DUCKDB_ENABLED:
        try:
            return _kpi_from_duckdb(since, until, group_name)
        except Exception:
            pass
    return _kpi_from_sqlite(since, until, group_name)

# --- HVDC Pipeline Endpoints ---
@app.get("/hvdc/status")
def get_hvdc_status(x_api_key: Optional[str] = Header(None, alias="X-API-Key")):
    """Get HVDC pipeline status and file information"""
    _require_api_key(x_api_key)
    return _get_hvdc_status()

@app.post("/hvdc/run")
def run_hvdc_pipeline(x_api_key: Optional[str] = Header(None, alias="X-API-Key")):
    """Run HVDC pipeline manually"""
    _require_api_key(x_api_key)
    return _run_hvdc_pipeline()

@app.post("/hvdc/transform")
def hvdc_transform(x_api_key: Optional[str] = Header(None, alias="X-API-Key")):
    """Execute transform.sql in DuckDB directly (cwd set to hvdc_logs)"""
    _require_api_key(x_api_key)

    if not HVDC_TRANSFORM_SQL.exists():
        return JSONResponse(status_code=500, content={
            "status": "error",
            "message": f"transform.sql not found at {HVDC_TRANSFORM_SQL.resolve()}"
        })

    try:
        import duckdb
        original_cwd = os.getcwd()
        sql_abs_path = HVDC_TRANSFORM_SQL if HVDC_TRANSFORM_SQL.is_absolute() else (Path(original_cwd) / HVDC_TRANSFORM_SQL).resolve()
        os.chdir(str(HVDC_BASE))
        try:
            conn = duckdb.connect(str(DUCKDB_PATH.absolute()))
            conn.execute(f"RUN '{sql_abs_path.as_posix()}'")
            conn.close()
        finally:
            os.chdir(original_cwd)

        return {"status": "ok", "message": f"transform.sql executed successfully at {sql_abs_path}"}

    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        return JSONResponse(status_code=500, content={
            "status": "error",
            "message": f"transform.sql execution failed: {str(e)}",
            "traceback": error_trace
        })

@app.get("/hvdc/kpi")
def get_hvdc_kpi(
    since: Optional[str] = None,
    until: Optional[str] = None,
    group_name: Optional[str] = None,
    x_api_key: Optional[str] = Header(None, alias="X-API-Key")
):
    """Get KPI from HVDC DuckDB pipeline (if available)"""
    _require_api_key(x_api_key)
    if DUCKDB_ENABLED:
        try:
            return _kpi_from_duckdb(since, until, group_name)
        except Exception as e:
            return {
                "status": "error",
                "message": f"DuckDB KPI query failed: {str(e)}",
                "fallback": "Use /kpi endpoint for SQLite-based KPI"
            }
    else:
        return {
            "status": "error",
            "message": "DuckDB not available",
            "fallback": "Use /kpi endpoint for SQLite-based KPI"
        }

# --- 새로운 자동화 엔드포인트들 ---
@app.get("/kpi/export.csv")
def export_kpi_csv(x_api_key: Optional[str] = Header(None, alias="X-API-Key")):
    """Export KPI as CSV stream"""
    _require_api_key(x_api_key)
    
    def rows():
        # SQLite에서 KPI 데이터 조회하여 CSV 변환
        conn = sqlite3.connect(str(SQLITE_PATH))
        cur = conn.cursor()
        cur.execute("""
            SELECT date_gst, group_name, sla_breaches, created_at
            FROM logs
            ORDER BY date_gst DESC
        """)
        yield "date_gst,group_name,sla_breaches,created_at\r\n"
        for r in cur.fetchall():
            buf = io.StringIO()
            csv.writer(buf).writerow(r)
            yield buf.getvalue()
        conn.close()
    
    return Response(content="".join(rows()), media_type="text/csv")

@app.get("/metrics")
def get_metrics(x_api_key: Optional[str] = Header(None, alias="X-API-Key")):
    """Get basic system metrics"""
    _require_api_key(x_api_key)
    
    return {
        "status": "ok",
        "uptime_seconds": float(time.time() - start_ts),
        "processed": 0,
        "queue_depth": 0,
        "duckdb_connected": bool(DUCKDB_ENABLED),
        "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    }
# ==== OpenAPI 스키마 강제 주입(로컬 전용) ====
from fastapi.openapi.utils import get_openapi

def _custom_openapi():
    """
    - servers: '/' (루트 오리진 https://localhost 하위로 인식)
    - securitySchemes: ApiKeyHeader + ApiKeyQuery 이중 지원
    - 모든 엔드포인트에 operationId 지정
    - components.schemas: 실제 API 응답과 일치하는 형태로 보강
    """
    if app.openapi_schema:
        return app.openapi_schema

    # FastAPI 기본 스키마 생성 후 커스텀 적용
    openapi_schema = get_openapi(
        title="HVDC WhatsApp → Local KPI Store (FastAPI + DuckDB Pipeline)",
        version="2.0.0",
        description="CSV/SQLite 저장 + 멱등성 + HMAC + KPI + DuckDB 연동 + HVDC Pipeline 통합. 로컬 전용 데이터 분석 시스템입니다.",
        routes=app.routes,
    )

    # 1) servers HTTPS로 설정 (루트 오리진 불일치 해결)
    openapi_schema["servers"] = [{"url": "https://localhost:8000", "description": "Local FastAPI over HTTPS"}]

    # 2) securitySchemes (Header + Query 이중 지원)
    openapi_schema.setdefault("components", {})
    openapi_schema["components"]["securitySchemes"] = {
        "ApiKeyHeader": {
            "type": "apiKey",
            "in": "header",
            "name": "X-API-Key",
            "description": "API 키를 헤더로 전송 (권장)"
        },
        "ApiKeyQuery": {
            "type": "apiKey",
            "in": "query",
            "name": "api_key",
            "description": "API 키를 쿼리 파라미터로 전송"
        }
    }
    # 전역 security (둘 중 하나만 사용하면 됨)
    openapi_schema["security"] = [{"ApiKeyHeader": []}, {"ApiKeyQuery": []}]

    # 3) 실제 API 응답과 일치하는 schemas 보강
    openapi_schema["components"].setdefault("schemas", {})
    schemas = openapi_schema["components"]["schemas"]
    schemas.update({
        "AppendLogRequest": {
            "type": "object",
            "required": ["date_gst", "group_name", "summary"],
            "properties": {
                "request_id": {
                    "type": "string", 
                    "description": "멱등성을 위한 고유 ID (UUID 권장)",
                    "example": "550e8400-e29b-41d4-a716-446655440000"
                },
                "date_gst": {
                    "type": "string", 
                    "pattern": "^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}$", 
                    "description": "GST 시간대의 날짜와 시간",
                    "example": "2025-08-09 10:00"
                },
                "group_name": {
                    "type": "string", 
                    "maxLength": 200, 
                    "description": "WhatsApp 그룹명",
                    "example": "Jopetwil 71 Group"
                },
                "summary": {
                    "type": "string", 
                    "maxLength": 5000, 
                    "description": "로그 요약 내용",
                    "example": "High tide paused offloading; resume at 08:00; next loading 10-Aug."
                },
                "top_keywords": {
                    "type": "array", 
                    "items": {"type": "string", "maxLength": 64},
                    "description": "주요 키워드 목록",
                    "example": ["High tide", "AGI", "Offloading"]
                },
                "sla_breaches": {
                    "type": "integer", 
                    "minimum": 0, 
                    "description": "SLA 위반 횟수",
                    "example": 0
                },
                "attachments": {
                    "type": "array", 
                    "items": {"type": "string"},
                    "description": "첨부파일 목록",
                    "example": []
                }
            }
        },
        "AppendResponse": {
            "type": "object",
            "properties": {
                "status": {"type": "string", "example": "ok"},
                "idempotency_key": {"type": "string", "description": "멱등성 키"},
                "attempt": {"type": "integer", "example": 1},
                "priority": {"type": "string", "example": "FYI"},
                "sla_breach": {"type": "integer", "example": 0},
                "message": {"type": "string", "example": "Stored in local CSV/SQLite"}
            }
        },
        "LogItem": {
            "type": "object",
            "properties": {
                "date_gst": {"type": "string", "example": "2025-08-09 10:00"},
                "group_name": {"type": "string", "example": "Jopetwil 71 Group"},
                "summary": {"type": "string", "example": "High tide paused offloading..."},
                "top_keywords": {"type": "array", "items": {"type": "string"}},
                "sla_breaches": {"type": "integer", "example": 0},
                "attachments": {"type": "array", "items": {"type": "string"}},
                "created_at": {"type": "string", "example": "2025-08-09T10:00:00"},
                "request_id": {"type": "string", "example": "550e8400-e29b-41d4-a716-446655440000"},
                "processed_status": {"type": "string", "example": "ok"}
            }
        },
        "LogsResponse": {
            "type": "object",
            "properties": {
                "status": {"type": "string", "example": "ok"},
                "rows": {"type": "array", "items": {"$ref": "#/components/schemas/LogItem"}},
                "timestamp": {"type": "string", "example": "2025-08-09T10:00:00"}
            }
        },
        "KpiItem": {
            "type": "object",
            "properties": {
                "date": {"type": "string", "example": "2025-08-09"},
                "group_name": {"type": "string", "example": "Jopetwil 71 Group"},
                "logs_count": {"type": "integer", "example": 2},
                "total_sla_breaches": {"type": "integer", "example": 0},
                "unique_keywords_count": {"type": "integer", "example": 3}
            }
        },
        "KpiResponse": {
            "type": "object",
            "properties": {
                "status": {"type": "string", "example": "ok"},
                "since": {"type": "string", "example": "2025-08-01"},
                "until": {"type": "string", "example": "2025-08-31"},
                "metrics": {"type": "array", "items": {"$ref": "#/components/schemas/KpiItem"}}
            }
        },
        "HvdcStatusResponse": {
            "type": "object",
            "properties": {
                "pipeline_script": {"type": "boolean", "example": True},
                "transform_sql": {"type": "boolean", "example": True},
                "bronze_data": {"type": "array", "items": {"type": "string"}, "example": ["sample_data.jsonl"]},
                "silver_data": {"type": "array", "items": {"type": "string"}, "example": ["data_0.parquet"]},
                "duckdb_file": {"type": "boolean", "example": True}
            }
        },
        "HvdcRunResponse": {
            "type": "object",
            "properties": {
                "status": {"type": "string", "example": "success"},
                "message": {"type": "string", "example": "HVDC pipeline executed successfully"},
                "stdout": {"type": "string", "example": "Starting HVDC Pipeline..."},
                "stderr": {"type": "string", "example": ""}
            }
        },
        "HealthResponse": {
            "type": "object",
            "properties": {
                "status": {"type": "string", "example": "ok"},
                "store": {
                    "type": "object",
                    "properties": {
                        "csv": {"type": "string", "example": "data/logs.csv"},
                        "sqlite": {"type": "string", "example": "data/logs.sqlite"}
                    }
                },
                "duckdb": {
                    "type": "object",
                    "properties": {
                        "enabled": {"type": "boolean", "example": True},
                        "path": {"type": "string", "example": "hvdc_logs/duckdb/hvdc.duckdb"}
                    }
                },
                "hvdc_pipeline": {"$ref": "#/components/schemas/HvdcStatusResponse"}
            }
        },
        "ErrorResponse": {
            "type": "object",
            "properties": {
                "status": {"type": "string", "example": "error"},
                "message": {"type": "string", "example": "Unauthorized"},
                "detail": {"type": "string", "example": "API key required"}
            }
        }
    })

    # 4) 모든 path+method에 operationId 지정
    def _ensure_op_id(path_item, method, default_id):
        op = path_item.get(method, {})
        if "operationId" not in op:
            op["operationId"] = default_id
            path_item[method] = op

    paths = openapi_schema.get("paths", {})
    for p, item in paths.items():
        if p == "/health":
            _ensure_op_id(item, "get", "getHealth")
        if p == "/logs":
            _ensure_op_id(item, "get", "getLogs")
            _ensure_op_id(item, "post", "appendLog")
        if p == "/kpi":
            _ensure_op_id(item, "get", "getKpi")
        if p == "/hvdc/status":
            _ensure_op_id(item, "get", "getHvdcStatus")
        if p == "/hvdc/run":
            _ensure_op_id(item, "post", "runHvdcPipeline")
        if p == "/hvdc/transform":
            _ensure_op_id(item, "post", "execTransformSql")
        if p == "/hvdc/kpi":
            _ensure_op_id(item, "get", "getHvdcKpi")
        if p == "/kpi/export.csv":
            _ensure_op_id(item, "get", "exportKpiCsv")
        if p == "/metrics":
            _ensure_op_id(item, "get", "getMetrics")

    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = _custom_openapi  # ← 등록 끝
# ==== /OpenAPI 스키마 강제 주입 ====


# ==== BEGIN: Hard-inject full OpenAPI spec (as-is) ====
import yaml

_OPENAPI_YAML = r"""
openapi: 3.1.1
info:
  title: HVDC WhatsApp → Local KPI Store (FastAPI + DuckDB)
  version: "2.0.0"
  description: |
    CSV/SQLite 저장 + 멱등성 + HMAC + KPI + DuckDB 연동.
    로컬 전용. Swagger/Viewer 호환을 위해 servers는 https://localhost 고정.

servers:
  - url: https://localhost
    description: Root origin 고정(포트/HTTP 금지)

components:
  securitySchemes:
    ApiKeyHeader:
      type: apiKey
      in: header
      name: X-API-Key
      description: "로컬 개발 키를 X-API-Key 헤더로 전달"

  schemas:
    AppendLogRequest:
      type: object
      required: [date_gst, group_name, summary]
      properties:
        request_id:
          type: string
          description: Idempotency key (UUID 권장)
          example: "550e8400-e29b-41d4-a716-446655440000"
        date_gst:
          type: string
          description: "GST local time, format: YYYY-MM-DD HH:mm"
          pattern: "^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}$"
          example: "2025-08-09 10:00"
        group_name:
          type: string
          maxLength: 200
          example: "Jopetwil 71 Group"
        summary:
          type: string
          maxLength: 5000
          example: "High tide paused offloading; resume 08:00 next day."
        top_keywords:
          type: array
          items: { type: string, maxLength: 64 }
          example: ["High tide","AGI","Offloading"]
        sla_breaches:
          type: integer
          minimum: 0
          example: 0
        attachments:
          type: array
          items: { type: string, description: "URL or filename" }
        signature:
          type: string
          description: "Optional HMAC-SHA256 Base64 of raw body"
    AppendResponse:
      type: object
      properties:
        status: { type: string, example: "ok" }
        idempotency_key: { type: string, example: "zQDLLv-azHJ3..." }
        attempt: { type: integer, example: 1 }
        priority: { type: string, example: "FYI" }
        sla_breach: { type: number, example: 0 }
        message: { type: string, example: "Queued for retry (q_xxx)"}
    GetRowsResponse:
      type: object
      properties:
        status: { type: string, example: "ok" }
        rows:
          type: array
          description: "시트 헤더 1행 + 데이터 행들. 각 행은 문자열 배열."
          items:
            type: array
            items:
              oneOf:
                - { type: string }
                - { type: number }
                - { type: integer }
        timestamp: { type: string, example: "2025-08-09 13:53:51" }
    KpiResponse:
      type: object
      properties:
        status: { type: string, example: "ok" }
        items:
          type: array
          items:
            type: object
            properties:
              date: { type: string, example: "2025-08-09" }
              group_name: { type: string, example: "Jopetwil 71 Group" }
              logs: { type: integer, example: 12 }
              sla_breaches: { type: integer, example: 0 }
    MetricsResponse:            # ← 뷰어 오류 원인 해결: proper object schema
      type: object
      required: [status, uptime_seconds]
      properties:
        status: { type: string, example: "ok" }
        uptime_seconds: { type: number, example: 1234.56 }
        processed: { type: integer, example: 42 }
        queue_depth: { type: integer, example: 0 }
        duckdb_connected: { type: boolean, example: true }
        timestamp: { type: string, example: "2025-08-09T13:55:00Z" }
    ErrorResponse:
      type: object
      properties:
        status: { type: string, example: "error" }
        message: { type: string, example: "Unauthorized" }

security:
  - ApiKeyHeader: []    # 뷰어 제약: 하나만 사용

paths:
  /health:
    get:
      operationId: getHealth
      tags: [Ops]
      summary: Health check
      responses:
        "200":
          description: OK
          content:
            application/json:
              schema: { $ref: "#/components/schemas/MetricsResponse" }

  /logs:
    post:
      operationId: appendLog
      tags: [Logs]
      summary: Append WhatsApp summary
      description: Logs 테이블(또는 CSV/SQLite)에 새 요약 추가. request_id 멱등.
      security: [ { ApiKeyHeader: [] } ]
      requestBody:
        required: true
        content:
          application/json:
            schema: { $ref: "#/components/schemas/AppendLogRequest" }
      responses:
        "200":
          description: OK (또는 큐 저장)
          content:
            application/json:
              schema: { $ref: "#/components/schemas/AppendResponse" }
        "400":
          description: Bad Request
          content:
            application/json:
              schema: { $ref: "#/components/schemas/ErrorResponse" }
        "401":
          description: Unauthorized
          content:
            application/json:
              schema: { $ref: "#/components/schemas/ErrorResponse" }

  /hvdc/run:
    post:
      operationId: runHvdc
      tags: [HVDC]
      summary: Run HVDC pipeline (Bronze→Silver)
      security: [ { ApiKeyHeader: [] } ]
      responses:
        "200":
          description: OK
          content:
            application/json:
              schema: { $ref: "#/components/schemas/MetricsResponse" }  # 파이프라인 결과 요약
        "500":
          description: Server Error
          content:
            application/json:
              schema: { $ref: "#/components/schemas/ErrorResponse" }

  /hvdc/transform:
    post:
      operationId: execTransformSql
      tags: [HVDC]
      summary: Execute transform.sql in DuckDB
      security: [ { ApiKeyHeader: [] } ]
      responses:
        "200":
          description: OK
          content:
            application/json:
              schema: { $ref: "#/components/schemas/MetricsResponse" }

  /kpi:
    get:
      operationId: getKpi
      tags: [KPI]
      summary: Query KPI (JSON)
      security: [ { ApiKeyHeader: [] } ]
      parameters:
        - name: since
          in: query
          required: false
          schema: { type: string }
          description: "YYYY-MM-DD 또는 ISO8601"
        - name: group_name
          in: query
          required: false
          schema: { type: string }
      responses:
        "200":
          description: OK
          content:
            application/json:
              schema: { $ref: "#/components/schemas/KpiResponse" }

  /kpi/export.csv:
    get:
      operationId: exportKpiCsv
      tags: [KPI]
      summary: Export KPI as CSV (stream)
      security: [ { ApiKeyHeader: [] } ]
      responses:
        "200":
          description: CSV stream
          content:
            text/csv:
              schema:
                type: string
                format: binary

  /metrics:
    get:
      operationId: getMetrics
      tags: [Ops]
      summary: Basic process metrics
      security: [ { ApiKeyHeader: [] } ]
      responses:
        "200":
          description: OK
          content:
            application/json:
              schema: { $ref: "#/components/schemas/MetricsResponse" }
"""

def _hard_injected_openapi():
    if getattr(app, "openapi_schema", None):
        return app.openapi_schema
    spec = yaml.safe_load(_OPENAPI_YAML)
    app.openapi_schema = spec
    return spec

# (삭제) 하드 인젝션 비활성화. 커스텀 빌더만 유지.
# app.openapi = _hard_injected_openapi
# ==== END: Hard-inject full OpenAPI spec (as-is) ====
