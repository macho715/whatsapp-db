# HVDC Logs Pipeline - DuckDB + Parquet

Windows 11 on ARM 환경에서 DuckDB와 Parquet을 활용한 로컬 데이터 분석 파이프라인입니다.

## 🏗️ 아키텍처

```
Bronze (JSONL) → Silver (Parquet) → Query/Report
     ↓              ↓              ↓
  원본 데이터    정제/파티셔닝    빠른 분석
```

## 📁 폴더 구조

```
hvdc_logs/
├── bronze/2025/08/          # 원본 JSONL 파일들
├── silver/logs/              # 파티션된 Parquet 파일들
├── duckdb/                   # DuckDB 데이터베이스 파일
├── transform.sql             # 변환 SQL 스크립트
├── run_pipeline.py          # Python 파이프라인 실행기
├── run_pipeline.bat         # Windows 배치 파일
└── README.md                # 이 파일
```

## 🚀 빠른 시작

### 1. 파이프라인 실행
```bash
# 더블클릭으로 실행
run_pipeline.bat

# 또는 Python으로 직접 실행
python run_pipeline.py
```

### 2. 수동 실행 (DuckDB CLI)
```bash
# DuckDB 연결
python -c "import duckdb; conn = duckdb.connect('duckdb/hvdc.duckdb'); print('Connected!')"

# SQL 스크립트 실행
python -c "import duckdb; conn = duckdb.connect('duckdb/hvdc.duckdb'); conn.execute(open('transform.sql').read())"
```

## 📊 데이터 형식

### Bronze (입력)
- **파일**: `YYYY-MM-DD_HH-MM-SS_group-name.jsonl`
- **형식**: JSON Lines (한 줄에 하나의 JSON 객체)
- **필수 필드**: `created_at`, `group_name`, `summary`

### Silver (출력)
- **파일**: `date=YYYY-MM-DD/group_name=.../*.parquet`
- **파티션**: 날짜별, 그룹별로 자동 분할
- **최적화**: 컬럼 지향 압축으로 빠른 쿼리

## 🔍 쿼리 예시

### 기본 KPI 조회
```sql
SELECT * FROM v_kpi_daily 
WHERE date BETWEEN '2025-08-01' AND '2025-08-31';
```

### 날짜별 로그 수
```sql
SELECT date, count(*) as logs
FROM read_parquet('silver/logs/**/*.parquet')
GROUP BY date
ORDER BY date DESC;
```

### SLA 위반 분석
```sql
SELECT group_name, sum(sla_breaches) as total_breaches
FROM read_parquet('silver/logs/**/*.parquet')
WHERE sla_breaches > 0
GROUP BY group_name;
```

## ⚡ 성능 최적화

- **파티션 프루닝**: 날짜/그룹 조건으로 불필요한 파일 스킵
- **컬럼 지향**: 필요한 컬럼만 읽기
- **압축**: Parquet 압축으로 저장 공간 절약
- **메모리**: DuckDB 자동 메모리 관리

## 🔧 설정 및 커스터마이징

### 메모리 제한 설정
```sql
SET memory_limit='8GB';  -- 16GB RAM 기준
```

### 스레드 수 설정
```sql
PRAGMA threads=12;  -- X1E 12코어 활용
```

### 파티션 전략 변경
`transform.sql`에서 `PARTITION_BY` 부분을 수정하여 다른 기준으로 파티셔닝 가능

## 📈 확장 가능성

- **클라우드 연동**: MotherDuck, S3, GCS 지원
- **실시간 처리**: 스트리밍 데이터 연동
- **시각화**: DuckDB + Streamlit/Panel 대시보드
- **ML 통합**: DuckDB ML 확장으로 예측 분석

## 🐛 문제 해결

### 일반적인 오류
1. **JSON 파싱 오류**: `created_at` 형식 확인 (YYYY-MM-DD HH:MM:SS)
2. **메모리 부족**: `memory_limit` 설정 조정
3. **파일 권한**: Windows 보안 설정 확인

### 로그 확인
- Python 스크립트 실행 시 상세 로그 출력
- DuckDB 오류 메시지 확인
- 파일 경로 및 권한 확인

## 📞 지원

- **DuckDB 공식 문서**: https://duckdb.org/docs/
- **Parquet 형식**: https://parquet.apache.org/
- **Python DuckDB**: `pip install duckdb`

---

**💡 팁**: 매일 새로운 JSONL 파일을 `bronze/2025/08/` 폴더에 넣고 `run_pipeline.bat`을 더블클릭하면 자동으로 변환됩니다!
