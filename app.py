"""
app.py — FastAPI HVDC WhatsApp → Local KPI Store
Features:
 - X-API-Key header auth
 - Idempotency-Key handling (SQLite)
 - Optional HMAC (X-Signature-256) verification
 - Persist to CSV + SQLite + Bronze JSONL
 - /hvdc/transform triggers WSL pipeline asynchronously (202 + job_id)
 - /hvdc/jobs/{job_id} job status
 - /kpi and /kpi/export.csv
Notes:
 - This server does NOT import duckdb in Windows runtime; DuckDB runs inside WSL hvdc311 venv.
 - Configure env vars: API_KEY, HMAC_SECRET, HVDC_LOGS_PATH (Windows path root for hvdc_logs)
"""

import os
import csv
import json
import hmac
import base64
import hashlib
import sqlite3
import uuid
import datetime
import threading
import subprocess
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, Header, HTTPException, BackgroundTasks, Request, Response, status
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

# ====== CONFIG via ENV ======
API_KEY = os.getenv("API_KEY", "changeme")          # production: override
HMAC_SECRET = os.getenv("HMAC_SECRET", "")          # optional; required for signature verify
HVDC_LOGS_PATH = os.getenv("HVDC_LOGS_PATH", r"C:\cursor-mcp\whatsapp db\hvdc_logs")
CSV_PATH = os.path.join(HVDC_LOGS_PATH, "data", "logs.csv")
SQLITE_PATH = os.path.join(HVDC_LOGS_PATH, "data", "sqlite", "kpi.sqlite")
BRONZE_DIR = os.path.join(HVDC_LOGS_PATH, "bronze")
PIPELINE_LOG_WIN = os.path.join(HVDC_LOGS_PATH, "pipeline_last_wsl.log")

# Ensure directories exist
os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
os.makedirs(os.path.dirname(SQLITE_PATH), exist_ok=True)
os.makedirs(BRONZE_DIR, exist_ok=True)
os.makedirs(HVDC_LOGS_PATH, exist_ok=True)

# Thread lock for CSV append
csv_lock = threading.Lock()

# ====== SQLite initialization (simple schema) ======
def init_sqlite():
    conn = sqlite3.connect(SQLITE_PATH, check_same_thread=False)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS logs (
      id TEXT PRIMARY KEY,
      date_gst TEXT,
      group_name TEXT,
      summary TEXT,
      top_keywords TEXT,
      sla_breaches INTEGER,
      attachments TEXT,
      created_at TEXT
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS idempotency (
      idempotency_key TEXT PRIMARY KEY,
      response_json TEXT,
      created_at TEXT
    )""")
    cur.execute("""
    CREATE TABLE IF NOT EXISTS jobs (
      job_id TEXT PRIMARY KEY,
      state TEXT,
      queued_at TEXT,
      started_at TEXT,
      finished_at TEXT,
      result_summary TEXT,
      error TEXT
    )""")
    conn.commit()
    return conn

_db_conn = init_sqlite()
_db_lock = threading.Lock()

# ====== Models ======
class AppendLogRequest(BaseModel):
    request_id: Optional[str] = Field(None, description="Idempotency key (UUID recommended)")
    date_gst: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$", example="2025-08-09 10:00")
    group_name: str = Field(..., max_length=200)
    summary: str = Field(..., max_length=5000)
    top_keywords: Optional[List[str]] = None
    sla_breaches: Optional[int] = 0
    attachments: Optional[List[str]] = None
    signature: Optional[str] = Field(None, description="Optional HMAC-SHA256 Base64 of raw body")

class AppendResponse(BaseModel):
    status: str
    idempotency_key: Optional[str] = None
    attempt: int = 1
    priority: str = "FYI"
    sla_breach: int = 0
    message: Optional[str] = None

class TransformAccepted(BaseModel):
    status: str
    job_id: str
    queued_at: str

class JobStatus(BaseModel):
    job_id: str
    state: str
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    result_summary: Optional[Dict[str, Any]] = None
    error: Optional[Dict[str, Any]] = None

# ====== FastAPI app ======
app = FastAPI(title="HVDC WhatsApp → Local KPI Store", version="2.1.2",
              description="Local-only API. CSV/SQLite storage + idempotency + HMAC header + KPI + DuckDB (via WSL).")

# ====== Helpers ======
def check_api_key(x_api_key: str = Header(None)):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

def verify_hmac(raw_body: bytes, header_sig: Optional[str]) -> bool:
    """
    header_sig is base64 of HMAC-SHA256
    """
    if not header_sig:
        return True if not HMAC_SECRET else False  # if secret configured, require header; else skip
    if not HMAC_SECRET:
        return True
    try:
        digest = hmac.new(HMAC_SECRET.encode('utf-8'), raw_body, hashlib.sha256).digest()
        sig_b64 = base64.b64encode(digest).decode('ascii')
        # timing-safe compare
        return hmac.compare_digest(sig_b64, header_sig)
    except Exception:
        return False

def write_csv_row(row: List[str]):
    with csv_lock:
        header_needed = not os.path.exists(CSV_PATH) or os.path.getsize(CSV_PATH) == 0
        with open(CSV_PATH, "a", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            if header_needed:
                writer.writerow(["id","date_gst","group_name","summary","top_keywords","sla_breaches","attachments","created_at"])
            writer.writerow(row)

def write_bronze_jsonl(obj: dict):
    fname = os.path.join(BRONZE_DIR, f"{datetime.datetime.utcnow().strftime('%Y-%m-%d')}_HVDC-Project-Lightning.jsonl")
    with open(fname, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(obj, ensure_ascii=False) + "\n")

def save_log_to_sqlite(req: AppendLogRequest, assigned_id: str):
    with _db_lock:
        cur = _db_conn.cursor()
        cur.execute("""
          INSERT OR REPLACE INTO logs (id,date_gst,group_name,summary,top_keywords,sla_breaches,attachments,created_at)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            assigned_id,
            req.date_gst,
            req.group_name,
            req.summary,
            json.dumps(req.top_keywords or []),
            req.sla_breaches or 0,
            json.dumps(req.attachments or []),
            datetime.datetime.utcnow().isoformat()+"Z"
        ))
        _db_conn.commit()

def set_idempotency(key: str, response: dict):
    with _db_lock:
        cur = _db_conn.cursor()
        cur.execute("INSERT OR REPLACE INTO idempotency (idempotency_key,response_json,created_at) VALUES (?, ?, ?)",
                    (key, json.dumps(response, ensure_ascii=False), datetime.datetime.utcnow().isoformat()+"Z"))
        _db_conn.commit()

def get_idempotent_response(key: str) -> Optional[dict]:
    with _db_lock:
        cur = _db_conn.cursor()
        cur.execute("SELECT response_json FROM idempotency WHERE idempotency_key = ?", (key,))
        r = cur.fetchone()
        if r:
            return json.loads(r[0])
        return None

def create_job(job_id: str):
    now = datetime.datetime.utcnow().isoformat()+"Z"
    with _db_lock:
        cur = _db_conn.cursor()
        cur.execute("INSERT OR REPLACE INTO jobs (job_id,state,queued_at) VALUES (?, ?, ?)", (job_id, "queued", now))
        _db_conn.commit()

def update_job(job_id: str, state: str, started_at=None, finished_at=None, result_summary=None, error=None):
    with _db_lock:
        cur = _db_conn.cursor()
        cur.execute("""
            UPDATE jobs SET state=?, started_at=?, finished_at=?, result_summary=?, error=? WHERE job_id=?
        """, (state, started_at, finished_at, json.dumps(result_summary) if result_summary else None, json.dumps(error) if error else None, job_id))
        _db_conn.commit()

def get_job(job_id: str) -> Optional[dict]:
    with _db_lock:
        cur = _db_conn.cursor()
        cur.execute("SELECT job_id,state,queued_at,started_at,finished_at,result_summary,error FROM jobs WHERE job_id=?", (job_id,))
        r = cur.fetchone()
        if not r:
            return None
        job = {
            "job_id": r[0],
            "state": r[1],
            "queued_at": r[2],
            "started_at": r[3],
            "finished_at": r[4],
            "result_summary": json.loads(r[5]) if r[5] else None,
            "error": json.loads(r[6]) if r[6] else None
        }
        return job

# ====== WSL pipeline runner (async) ======
def run_wsl_pipeline_job(job_id: str):
    """
    Run the pipeline inside WSL hvdc311 venv.
    This function should be called from BackgroundTasks (non-blocking from request).
    It updates job status in sqlite.
    """
    try:
        update_job(job_id, "running", started_at=datetime.datetime.utcnow().isoformat()+"Z")
        # Build the WSL command carefully (quote Windows path)
        # Notice double-quoting: use bash -lc '...'
        wsl_cmd = (
            "wsl -d Ubuntu -- bash -lc "
            + "\"source ~/hvdc311/bin/activate && "
            + "cd '/mnt/c/cursor-mcp/whatsapp db/hvdc_logs' && "
            + "python3 run_pipeline.py >> '/mnt/c/cursor-mcp/whatsapp db/hvdc_logs/pipeline_last_wsl.log' 2>&1\""
        )
        # Start process via powershell wrapper to allow proper quoting on Windows
        # Use shell=True intentionally for complex quoting on Windows; Popen returns immediately
        proc = subprocess.Popen(["powershell", "-Command", wsl_cmd], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        # We won't block here; instead, do a polling loop to detect completion (optional).
        # Simple approach: poll job logfile for completion indicator OR wait for process.
        proc.wait(timeout=900)  # wait up to 15 minutes; adjust as needed
        # after completion, mark succeeded (we could parse logs for errors)
        update_job(job_id, "succeeded", finished_at=datetime.datetime.utcnow().isoformat()+"Z",
                   result_summary={"message":"pipeline finished (check pipeline_last_wsl.log)"})
    except subprocess.TimeoutExpired:
        update_job(job_id, "failed", finished_at=datetime.datetime.utcnow().isoformat()+"Z",
                   error={"message": "timeout", "detail": "Pipeline exceeded timeout"})
    except Exception as e:
        update_job(job_id, "failed", finished_at=datetime.datetime.utcnow().isoformat()+"Z",
                   error={"message": str(e)})

# ====== API Endpoints ======

@app.get("/health", response_model=dict)
def get_health(x_api_key: str = Header(None)):
    check_api_key(x_api_key)
    # compute some metrics
    with _db_lock:
        cur = _db_conn.cursor()
        cur.execute("SELECT COUNT(1) FROM logs")
        processed = cur.fetchone()[0] or 0
        cur.execute("SELECT COUNT(1) FROM jobs WHERE state='queued'")
        queue_depth = cur.fetchone()[0] or 0
    uptime_seconds = int((datetime.datetime.utcnow() - datetime.datetime.utcfromtimestamp(os.path.getmtime(__file__))).total_seconds()) if os.path.exists(__file__) else 0
    # Check WSL DuckDB health by attempting a short WSL python invocation (non-fatal)
    duckdb_connected = False
    try:
        check_cmd = ["powershell", "-Command",
                     "wsl -d Ubuntu -- python3 -c \"import duckdb; duckdb.connect(':memory:').execute('select 1')\""]
        subprocess.run(check_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10, check=True)
        duckdb_connected = True
    except Exception:
        duckdb_connected = False

    return {
        "status": "ok",
        "uptime_seconds": float(uptime_seconds),
        "processed": int(processed),
        "queue_depth": int(queue_depth),
        "duckdb_connected": bool(duckdb_connected),
        "timestamp": datetime.datetime.utcnow().isoformat()+"Z"
    }

@app.post("/logs", response_model=AppendResponse)
async def append_log(request: Request,
                     payload: AppendLogRequest,
                     x_api_key: str = Header(None),
                     idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
                     x_signature_256: Optional[str] = Header(None, alias="X-Signature-256")):
    # Auth
    check_api_key(x_api_key)

    # idempotency key fallback: body.request_id OR header
    final_idem = idempotency_key or payload.request_id or str(uuid.uuid4())

    # check idempotency store
    prev = get_idempotent_response(final_idem)
    if prev:
        # Return stored response
        return JSONResponse(status_code=200, content=prev)

    # verify HMAC signature if configured
    raw_body = await request.body()
    if HMAC_SECRET:
        ok = verify_hmac(raw_body, x_signature_256 or payload.signature)
        if not ok:
            raise HTTPException(status_code=401, detail="Invalid signature")

    # generate assigned id
    assigned_id = payload.request_id or str(uuid.uuid4())
    # persist: CSV, SQLite, Bronze JSONL
    try:
        write_csv_row([assigned_id, payload.date_gst, payload.group_name, payload.summary,
                       json.dumps(payload.top_keywords or []), payload.sla_breaches or 0,
                       json.dumps(payload.attachments or []), datetime.datetime.utcnow().isoformat()+"Z"])
        save_log_to_sqlite(payload, assigned_id)

        # Bronze JSONL record (mirror)
        bronze_obj = {
            "id": assigned_id,
            "date_gst": payload.date_gst,
            "group_name": payload.group_name,
            "summary": payload.summary,
            "top_keywords": payload.top_keywords or [],
            "sla_breaches": payload.sla_breaches or 0,
            "attachments": payload.attachments or [],
            "created_at": datetime.datetime.utcnow().isoformat()+"Z"
        }
        write_bronze_jsonl(bronze_obj)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Storage error: {e}")

    response_payload = {
        "status": "ok",
        "idempotency_key": final_idem,
        "attempt": 1,
        "priority": "FYI",
        "sla_breach": payload.sla_breaches or 0,
        "message": "Stored in local CSV/SQLite + Bronze JSONL"
    }
    # persist idempotency mapping
    set_idempotency(final_idem, response_payload)

    # Optionally: trigger transform/run asynchronously here (not automatic unless desired)
    return JSONResponse(status_code=200, content=response_payload)

@app.post("/hvdc/transform", status_code=202, response_model=TransformAccepted)
def exec_transform_async(background: BackgroundTasks,
                         x_api_key: str = Header(None),
                         idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key"),
                         x_signature_256: Optional[str] = Header(None, alias="X-Signature-256")):
    """
    Trigger transform.sql via WSL DuckDB (async).
    Returns 202 Accepted + job_id immediately. Background task will run pipeline in WSL.
    """
    check_api_key(x_api_key)
    # idempotency handling: ensure same idempotency_key returns same job (if exists)
    job_key = idempotency_key or str(uuid.uuid4())
    existing = get_idempotent_response(job_key)
    if existing:
        # If previously accepted, return same job_id
        return JSONResponse(status_code=202, content=existing)

    job_id = str(uuid.uuid4())
    create_job(job_id)
    background.add_task(run_wsl_pipeline_job, job_id)

    resp = {"status": "accepted", "job_id": job_id, "queued_at": datetime.datetime.utcnow().isoformat()+"Z"}
    set_idempotency(job_key, resp)
    return resp

@app.get("/hvdc/jobs/{job_id}", response_model=JobStatus)
def get_transform_job(job_id: str, x_api_key: str = Header(None)):
    check_api_key(x_api_key)
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return JobStatus(
        job_id=job["job_id"],
        state=job["state"],
        started_at=job["started_at"],
        finished_at=job["finished_at"],
        result_summary=job["result_summary"],
        error=job["error"]
    )

@app.post("/hvdc/run")
def run_hvdc(x_api_key: str = Header(None)):
    """
    Optionally synchronous pipeline run (blocking). Use with caution.
    This will call WSL and wait (up to configured timeout).
    """
    check_api_key(x_api_key)
    job_id = str(uuid.uuid4())
    create_job(job_id)
    try:
        update_job(job_id, "running", started_at=datetime.datetime.utcnow().isoformat()+"Z")
        # Run pipeline synchronously (blocking)
        wsl_cmd = (
            "wsl -d Ubuntu -- bash -lc "
            + "\"source ~/hvdc311/bin/activate && cd '/mnt/c/cursor-mcp/whatsapp db/hvdc_logs' && python3 run_pipeline.py\""
        )
        subprocess.run(["powershell", "-Command", wsl_cmd], check=True, timeout=900)
        update_job(job_id, "succeeded", finished_at=datetime.datetime.utcnow().isoformat()+"Z",
                   result_summary={"message": "sync pipeline finished"})
        return JSONResponse(status_code=200, content={"status":"ok"})
    except subprocess.CalledProcessError as e:
        update_job(job_id, "failed", finished_at=datetime.datetime.utcnow().isoformat()+"Z", error={"cmd": str(e)})
        raise HTTPException(status_code=500, detail="Pipeline failed")
    except subprocess.TimeoutExpired:
        update_job(job_id, "failed", finished_at=datetime.datetime.utcnow().isoformat()+"Z", error={"message":"timeout"})
        raise HTTPException(status_code=500, detail="Pipeline timeout")

@app.get("/kpi")
def get_kpi(since: Optional[str] = None, group_name: Optional[str] = None, x_api_key: str = Header(None)):
    check_api_key(x_api_key)
    # simple aggregation from logs table
    q = "SELECT substr(date_gst,1,10) as dt, group_name, COUNT(1) as logs, SUM(sla_breaches) as sla_breaches FROM logs WHERE 1=1"
    params = []
    if since:
        q += " AND date_gst >= ?"
        params.append(since)
    if group_name:
        q += " AND group_name = ?"
        params.append(group_name)
    q += " GROUP BY dt, group_name ORDER BY dt DESC LIMIT 1000"
    with _db_lock:
        cur = _db_conn.cursor()
        cur.execute(q, tuple(params))
        rows = cur.fetchall()
    items = [{"date": r[0], "group_name": r[1], "logs": int(r[2] or 0), "sla_breaches": int(r[3] or 0)} for r in rows]
    return {"status":"ok", "items": items}

@app.get("/kpi/export.csv")
def export_kpi_csv(since: Optional[str] = None, group_name: Optional[str] = None, x_api_key: str = Header(None)):
    check_api_key(x_api_key)
    # stream CSV
    def iter_csv():
        yield "date,group_name,logs,sla_breaches\r\n"
        q = "SELECT substr(date_gst,1,10) as dt, group_name, COUNT(1) as logs, SUM(sla_breaches) as sla_breaches FROM logs WHERE 1=1"
        params = []
        if since:
            q += " AND date_gst >= ?"
            params.append(since)
        if group_name:
            q += " AND group_name = ?"
            params.append(group_name)
        q += " GROUP BY dt, group_name ORDER BY dt DESC"
        with _db_lock:
            cur = _db_conn.cursor()
            cur.execute(q, tuple(params))
            for r in cur:
                yield f"{r[0]},{r[1]},{r[2] or 0},{r[3] or 0}\r\n"
    return StreamingResponse(iter_csv(), media_type="text/csv")

# ====== Run uvicorn if executed directly ======
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8010, reload=True)
