# DeepTutor Web — Startup Script
# Starts: Next.js standalone + proxy + verifies backend containers
# Run from repo root: pwsh -File scripts/start-web.ps1

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent (Split-Path -Parent $PSCommandPath)
$WebRoot = "$RepoRoot/web"
# Use .next2 build (newer than .next which is locked)
$BuildDir = ".next2"
Write-Host "=== DeepTutor Web Startup ===" -ForegroundColor Cyan

# 1. Verify backend containers are healthy (compose-managed)
$Backend = docker ps --filter "name=deeptutor" --filter "status=running" --format "{{.Status}}" 2>$null
$Platform = docker ps --filter "name=platform" --filter "status=running" --format "{{.Status}}" 2>$null

if (-not $Backend) { Write-Host "WARNING: deeptutor not running, run: docker compose up -d" -ForegroundColor Yellow }
if (-not $Platform) { Write-Host "WARNING: platform not running, run: docker compose up -d" -ForegroundColor Yellow }

# 2. Copy static chunks if missing (standalone build quirk)
$StaticSrc = "$WebRoot/$BuildDir/static"
$StaticDst = "$WebRoot/$BuildDir/standalone/$BuildDir/static"
if (-not (Test-Path "$StaticDst/chunks")) {
    if (Test-Path $StaticSrc) {
        Write-Host "Copying static chunks to standalone..." -ForegroundColor Cyan
        Copy-Item -Recurse -Path "$StaticSrc\*" -Destination $StaticDst -Force
    }
}

# 3. Kill any stale processes on our ports
foreach ($port in @(3783, 3782)) {
    $pid = netstat -ano | Select-String ":$port\s" | Select-String "LISTENING" | ForEach-Object { $_ -replace '.*\s+(\d+)$', '$1' } | Select-Object -First 1
    if ($pid) {
        Write-Host "Killing stale process PID $pid on port $port" -ForegroundColor Yellow
        taskkill /F /PID $pid 2>$null
        Start-Sleep 1
    }
}

# 4. Start Next.js standalone on :3783
Write-Host "Starting Next.js standalone on :3783..." -ForegroundColor Cyan
$env:PORT = "3783"
$env:HOSTNAME = "0.0.0.0"
$Job1 = Start-Job -ScriptBlock {
    Set-Location "$using:WebRoot/.next2/standalone"
    node server.js
}

# 5. Wait for Next.js to start
Start-Sleep 3

# 6. Start proxy on :3782
Write-Host "Starting proxy on :3782..." -ForegroundColor Cyan
$Job2 = Start-Job -ScriptBlock {
    Set-Location "$using:RepoRoot"
    node scripts/frontend-proxy.mjs
}

# 7. Verify
Start-Sleep 2
$Test = curl.exe -s -o nul -w "%{http_code}" http://localhost:3782/ 2>$null
if ($Test -eq "200") {
    Write-Host "✓ Web UI ready at http://localhost:3782" -ForegroundColor Green
} else {
    Write-Host "✗ Web UI returned $Test" -ForegroundColor Red
}

Write-Host "Press Ctrl+C to stop all services" -ForegroundColor Cyan

# Wait for jobs (until Ctrl+C)
Wait-Job $Job1, $Job2
