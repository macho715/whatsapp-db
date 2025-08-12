@echo off
REM ===============================
REM UAE HVDC KPI 변환·조회·CSV 확인 자동 실행 스크립트
REM ===============================

SETLOCAL ENABLEDELAYEDEXPANSION

REM API 기본 설정
SET API_URL=http://127.0.0.1:8006/hvdc
SET API_KEY=dev

echo [1/3] 변환 실행...
curl -s -X POST %API_URL%/transform -H "X-API-Key: %API_KEY%"
echo.

echo [2/3] KPI 조회...
curl -s %API_URL%/kpi -H "X-API-Key: %API_KEY%"
echo.

echo [3/3] CSV 확인...
IF EXIST "hvdc_logs\kpi_report.csv" (
    type hvdc_logs\kpi_report.csv
) ELSE (
    echo CSV 파일이 존재하지 않습니다: hvdc_logs\kpi_report.csv
)

echo.
echo ====== 작업 완료 ======
pause
