@echo off
REM DeepTutor Web Frontend — starts proxy + Next.js in background
REM Called by Windows scheduled task at logon.
cd /d "%~dp0..\web\.next2\standalone"

REM Copy static chunks if missing (standalone build quirk)
if not exist ".next2\static\chunks" (
  if exist "..\..\.next2\static" (
    xcopy /E /I /Y "..\..\.next2\static" ".next2\static" >nul
  )
)

set PORT=3783
set HOSTNAME=0.0.0.0
start /B node server.js
timeout /t 3 /nobreak >nul
cd /d "%~dp0.."
start /B node scripts/frontend-proxy.mjs
