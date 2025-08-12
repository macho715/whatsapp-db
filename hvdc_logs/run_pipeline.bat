@echo off
echo ========================================
echo    HVDC Logs Pipeline Runner
echo    Bronze → Silver → Query
echo ========================================
echo.

cd /d "%~dp0"

echo 🚀 Starting HVDC Pipeline...
echo 📁 Current directory: %CD%
echo.

echo 🔄 Running Python pipeline...
python run_pipeline.py

echo.
echo ✅ Pipeline completed!
echo 📊 Check the results in the silver/logs directory
echo 📄 CSV report generated as kpi_report.csv
echo.

pause
