#!/bin/bash
set -e

BACKEND_PORT=${BACKEND_PORT:-8001}

echo "[Backend]  🚀 Starting FastAPI backend on port ${BACKEND_PORT}..."

# Run uvicorn directly - the application's logging system already handles:
# 1. Console output (visible in docker logs)
# 2. File logging to data/user/logs/ai_tutor_*.log
exec python -m uvicorn deeptutor.api.main:app --host 0.0.0.0 --port ${BACKEND_PORT}
