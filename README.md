# WhatsApp → HVDC Local KPI Store (Hybrid Windows + WSL DuckDB)

- Windows에서 FastAPI(API) + CSV/SQLite로 무중단 수집/조회
- DuckDB 기반 ETL/OLAP은 WSL(Ubuntu) venv에서 실행(하이브리드)
- `/hvdc/transform`은 WSL 파이프라인 비동기 트리거(HTTP 202)

## 구성
- Windows(API): `main.py` – `/health`, `/logs`, `/kpi`, `/hvdc/transform(202)`
- 저장소: `data/logs.csv`, `data/sqlite`, 토글 `fallback_sqlite.ON`
- WSL(ETL/OLAP): venv `hvdc311`, DuckDB `hvdc_logs/duckdb/hvdc.duckdb`

## 빠른 시작
### Windows API
```powershell
.\.venv312\Scripts\activate
python -m uvicorn main:app --host 127.0.0.1 --port 8010 --log-level info
```
- 헬스: `GET /health`
- 로그: `POST /logs`
- KPI(SQLite Fallback): `GET /kpi`
- 변환 트리거: `POST /hvdc/transform` → 202

### WSL DuckDB 파이프라인
```bash
source ~/hvdc311/bin/activate
cd "/mnt/c/cursor-mcp/whatsapp db/hvdc_logs"
python3 run_pipeline.py
```

## 스케줄/자동화
- Task Scheduler → `run_wsl_pipeline.ps1` (WSL 파이프라인 주기 실행)
- DuckDB 헬스(15m): `duckdb_health_check.py` (성공 시 토글 제거)

## Git
- origin: 기존 원격, secondary: `https://github.com/macho715/whatsapp-db.git`
```bash
git remote -v
git push origin main
git push secondary main
```
