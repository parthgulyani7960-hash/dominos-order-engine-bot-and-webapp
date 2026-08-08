@echo off
title Domino's Order Engine Platform
echo ====================================================
echo   Domino's Order Engine v2.0 Platform Launcher
echo ====================================================
echo   * Launches FastAPI Web Server on port 8000
echo   * Starts Telegram Bot polling in background
echo ====================================================
echo.

REM Use py -3 for explicit Python 3, pipe stderr separately to avoid noise
py -3 -m uvicorn app.backend.main:app --host 0.0.0.0 --port 8000 --log-level info

pause
