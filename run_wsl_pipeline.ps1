$ErrorActionPreference = 'Stop'
$project = (Get-Location).Path
$logPath = Join-Path $project 'hvdc_logs\pipeline_last_wsl.log'

$wslCmd = "source ~/hvdc311/bin/activate && cd '/mnt/c/cursor-mcp/whatsapp db/hvdc_logs' && python3 run_pipeline.py"

try {
	# Execute pipeline in WSL and capture output
	$output = wsl -d Ubuntu -- bash -lc $wslCmd 2>&1
	$output | Out-File -FilePath $logPath -Encoding utf8
	# On success, remove fallback toggle
	Remove-Item -Force (Join-Path $project 'fallback_sqlite.ON') -ErrorAction SilentlyContinue
}
catch {
	# On failure, keep fallback; append error to log
	"`n[ERROR] $($_.Exception.Message)" | Out-File -FilePath $logPath -Append -Encoding utf8
}


