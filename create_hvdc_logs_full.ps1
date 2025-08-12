# =========================================
# HVDC Logs 완전 패키지 생성 스크립트
# =========================================

# 기본 경로
$basePath = "C:\cursor-mcp\whatsapp db\hvdc_logs"

Write-Host "📁 생성 경로:" $basePath

# 폴더 구조
$dirs = @(
    "$basePath",
    "$basePath\input",
    "$basePath\duckdb",
    "$basePath\notebooks"
)
foreach ($dir in $dirs) {
    if (-Not (Test-Path -Path $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
        Write-Host "✅ 폴더 생성:" $dir
    } else {
        Write-Host "ℹ️ 이미 존재:" $dir
    }
}

# __init__.py
$initFile = "$basePath\__init__.py"
if (-Not (Test-Path -Path $initFile)) {
    New-Item -ItemType File -Path $initFile -Force | Out-Null
    Write-Host "✅ __init__.py 생성됨"
} else {
    Write-Host "ℹ️ __init__.py 이미 존재"
}

# run_ui_dashboard.bat
$batFile = "$basePath\run_ui_dashboard.bat"
$batContent = '@echo off
duckdb -ui "%~dp0notebooks\sla_kpi_dashboard.duckdbsql"'
Set-Content -Path $batFile -Value $batContent -Encoding UTF8
Write-Host "✅ run_ui_dashboard.bat 생성됨"

# run_ui_dashboard.sh
$shFile = "$basePath\run_ui_dashboard.sh"
$shContent = '#!/bin/bash
duckdb -ui "$(dirname "$0")/notebooks/sla_kpi_dashboard.duckdbsql"'
Set-Content -Path $shFile -Value $shContent -Encoding UTF8
Write-Host "✅ run_ui_dashboard.sh 생성됨"

# bronze_stage.py
$bronzeFile = "$basePath\bronze_stage.py"
$bronzeContent = @"
import duckdb, os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DUCKDB_FILE = os.path.join(BASE_DIR, 'duckdb', 'hvdc.duckdb')
INPUT_DIR = os.path.join(BASE_DIR, 'input')

def load_csv_to_duckdb():
    con = duckdb.connect(DUCKDB_FILE)
    con.execute(f'''
        CREATE OR REPLACE TABLE raw_logs AS
        SELECT * FROM read_csv_auto('{INPUT_DIR.replace("\\", "/")}/*.csv', ignore_errors=true)
    ''')
    con.close()
    print('[BRONZE] CSV loaded to raw_logs')

if __name__ == '__main__':
    load_csv_to_duckdb()
"@
Set-Content -Path $bronzeFile -Value $bronzeContent -Encoding UTF8
Write-Host "✅ bronze_stage.py 생성됨"

# silver_stage.py
$silverFile = "$basePath\silver_stage.py"
$silverContent = @"
import duckdb, os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DUCKDB_FILE = os.path.join(BASE_DIR, 'duckdb', 'hvdc.duckdb')

def transform_raw_to_sla():
    con = duckdb.connect(DUCKDB_FILE)
    con.execute('''
        CREATE OR REPLACE TABLE sla_log AS
        SELECT date_gst, group_name, sender, sender_role,
               message, tags, top_keywords, sla_breaches, attachments
        FROM raw_logs
        WHERE date_gst IS NOT NULL
    ''')
    con.close()
    print('[SILVER] raw_logs transformed to sla_log')

if __name__ == '__main__':
    transform_raw_to_sla()
"@
Set-Content -Path $silverFile -Value $silverContent -Encoding UTF8
Write-Host "✅ silver_stage.py 생성됨"

# pipeline_sequence.py
$pipelineFile = "$basePath\pipeline_sequence.py"
$pipelineContent = @"
import subprocess, os, duckdb

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DUCKDB_FILE = os.path.join(BASE_DIR, 'duckdb', 'hvdc.duckdb')
TRANSFORM_SQL = os.path.join(BASE_DIR, 'transform.sql')

def run_pipeline_sequence():
    subprocess.run(['python', os.path.join(BASE_DIR, 'bronze_stage.py')], check=True)
    subprocess.run(['python', os.path.join(BASE_DIR, 'silver_stage.py')], check=True)

    con = duckdb.connect(DUCKDB_FILE)
    with open(TRANSFORM_SQL, 'r', encoding='utf-8') as f:
        sql_script = f.read()
    con.execute(sql_script)
    con.close()
    print('[PIPELINE] Bronze → Silver → Transform completed')
    return {'status': 'ok'}

if __name__ == '__main__':
    run_pipeline_sequence()
"@
Set-Content -Path $pipelineFile -Value $pipelineContent -Encoding UTF8
Write-Host "✅ pipeline_sequence.py 생성됨"

# transform.sql
$transformFile = "$basePath\transform.sql"
$transformContent = @"
CREATE OR REPLACE VIEW v_kpi_daily AS
SELECT DATE_TRUNC('day', date_gst) AS date,
       group_name,
       COUNT(*) AS logs,
       SUM(sla_breaches) AS sla_breaches
FROM sla_log
GROUP BY DATE_TRUNC('day', date_gst), group_name
ORDER BY date DESC;

COPY (SELECT * FROM v_kpi_daily)
TO 'kpi_report.csv' (HEADER, DELIMITER ',');
"@
Set-Content -Path $transformFile -Value $transformContent -Encoding UTF8
Write-Host "✅ transform.sql 생성됨"

# 기본 노트북
$dashboardFile = "$basePath\notebooks\sla_kpi_dashboard.duckdbsql"
$dashboardContent = "-- SLA KPI Dashboard 기본 노트북"
Set-Content -Path $dashboardFile -Value $dashboardContent -Encoding UTF8
Write-Host "✅ sla_kpi_dashboard.duckdbsql 생성됨"

Write-Host "🎯 HVDC Logs 완전 패키지 생성 완료!"
