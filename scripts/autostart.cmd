@echo off
REM DeepTutor Auto-Start — run at Windows logon to ensure all services are up
cd /d "%~dp0.."

echo [DeepTutor] Waiting for Docker...
:wait_docker
docker ps >nul 2>&1
if errorlevel 1 (
    timeout /t 5 /nobreak >nul
    goto wait_docker
)

echo [DeepTutor] Docker ready. Ensuring containers...
docker compose -f docker-compose.yml up -d 2>nul

echo [DeepTutor] Starting web frontend...
pwsh -File scripts/start-web.ps1
