# ================================
# HVDC API 테스트 스크립트 (PowerShell)
# ================================

$ApiKey = "dev"
$ApiHost = "127.0.0.1"
$ApiPort = "8010"   # uvicorn 실행 포트

Write-Host "🚀 HVDC API 테스트 시작 (HOST=$ApiHost PORT=$ApiPort API_KEY=$ApiKey)"
Write-Host ""

# 1) 전체 파이프라인 실행
Write-Host "▶ 1. /automate/test-pipeline"
curl.exe -s -X POST "http://$ApiHost`:$ApiPort/automate/test-pipeline" -H "X-API-Key: $ApiKey"
Write-Host ""
Write-Host ""

# 2) KPI 조회
Write-Host "▶ 2. /hvdc/kpi"
curl.exe -s "http://$ApiHost`:$ApiPort/hvdc/kpi" -H "X-API-Key: $ApiKey"
Write-Host ""
Write-Host ""

# 3) Transform 실행
Write-Host "▶ 3. /hvdc/transform"
curl.exe -s -X POST "http://$ApiHost`:$ApiPort/hvdc/transform" -H "X-API-Key: $ApiKey"
Write-Host ""
Write-Host "✅ 테스트 완료"
