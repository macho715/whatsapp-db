$ErrorActionPreference = "Stop"

# Project base (adjust if different)
$base = "C:\cursor-mcp\whatsapp db"
$hvdcLogs = Join-Path $base "hvdc_logs"
$toggle = Join-Path $base "fallback_sqlite.ON"
$pipelineLog = Join-Path $hvdcLogs "pipeline_last_wsl.log"

Write-Host "[WSL] Triggering DuckDB pipeline..." -ForegroundColor Cyan

$wslCmd = "set -e; source ~/hvdc311/bin/activate; cd '/mnt/c/cursor-mcp/whatsapp db/hvdc_logs'; python3 run_pipeline.py"

try {
    wsl -d Ubuntu -- bash -lc $wslCmd | Tee-Object -FilePath $pipelineLog -Append | Out-Null
    $exitCode = $LASTEXITCODE
} catch {
    $exitCode = 1
    Write-Warning "WSL pipeline threw an exception: $($_.Exception.Message)"
}

if ($exitCode -eq 0) {
    Write-Host "[WSL] Pipeline: SUCCESS" -ForegroundColor Green
    if (Test-Path $toggle) {
        Remove-Item $toggle -Force -ErrorAction SilentlyContinue
        Write-Host "Removed toggle: $toggle"
    }
} else {
    Write-Warning "[WSL] Pipeline: FAIL (exit=$exitCode)"
    New-Item -Path $toggle -ItemType File -Force | Out-Null
    Write-Host "Ensured toggle exists: $toggle"
}

Write-Host "\n== Tail pipeline log ==" -ForegroundColor Yellow
if (Test-Path $pipelineLog) {
    Get-Content $pipelineLog -Tail 120 | Write-Host
} else {
    Write-Host "No pipeline log found at $pipelineLog"
}

Write-Host "\n== API Health ==" -ForegroundColor Yellow
try {
    $resp = Invoke-RestMethod -Uri "http://127.0.0.1:8010/health" -Headers @{ 'X-API-Key' = 'changeme' } -TimeoutSec 5
    $resp | ConvertTo-Json -Depth 5 | Write-Host
} catch {
    Write-Warning "Health check failed: $($_.Exception.Message)"
}

Write-Host "\nDone." -ForegroundColor Cyan


