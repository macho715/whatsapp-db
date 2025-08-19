@app.py 아주 좋습니다 — 아래는 **로컬 전체 FastAPI 서버 코드(단일 파일: `app.py`)**, 필요한 `requirements.txt`, 실행·설정 방법, 그리고 검증/운영 팁입니다.
빠르게 정리: 이 구현은 (1) X-API-Key, (2) Idempotency-Key, (3) optional HMAC(X-Signature-256), (4) CSV+SQLite, (5) Bronze JSONL, (6) 비동기 WSL DuckDB 트리거(202), (7) job 조회, (8) /kpi 및 CSV export 포함.

---

## 파일 1 — `app.py`

- 전체 코드는 저장소의 `app.py`를 사용하세요. Swagger UI: `http://127.0.0.1:8010/docs`

---

## 파일 2 — `requirements.txt`

권장 최소 사양(프로젝트 루트의 실제 requirements.txt는 더 포괄적입니다):
```
fastapi>=0.95
uvicorn[standard]>=0.22
pydantic>=1.10
```

---

## 실행 & 설정

1) 환경변수 설정
```powershell
setx API_KEY "your_api_key_here"
setx HMAC_SECRET "your_hmac_secret_here"    # optional
setx HVDC_LOGS_PATH "C:\cursor-mcp\whatsapp db\hvdc_logs"
```

2) 설치 & 실행
```bash
python -m pip install -r requirements.txt
uvicorn app:app --host 127.0.0.1 --port 8010 --reload
```

---

## 운영 팁
- BackgroundTasks로 WSL 파이프라인 비동기 트리거
- `fallback_sqlite.ON` 존재 시 KPI는 SQLite 경로 고정
- 로그/데이터 경로에 공백이 있어 경로는 항상 따옴표 처리

---

레퍼런스: FastAPI BackgroundTasks, RFC7807, Stripe Idempotency, GitHub Webhook HMAC, WSL 호출 예제
