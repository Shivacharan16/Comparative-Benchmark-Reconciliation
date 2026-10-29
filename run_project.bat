@echo off
title Data Engineering Pipeline - Ingestion Benchmark
color 0A
cd /d "D:\yotta_flex_ai project"
echo ======================================================================
echo  Launching VS Code and Running Data Engineering Pipeline Benchmark...
echo ======================================================================
echo.

rem Launch VS Code in background opening the project
start "" "C:\Users\bshiv\AppData\Local\Programs\Microsoft VS Code\Code.exe" "D:\yotta_flex_ai project" "D:\yotta_flex_ai project\run_demo.py"

rem Execute the pipeline in this visible terminal
call .\.venv\Scripts\activate.bat
python run_demo.py

echo.
echo ======================================================================
echo  Pipeline Execution Finished! All reports saved in reports\ directory.
echo ======================================================================
pause
