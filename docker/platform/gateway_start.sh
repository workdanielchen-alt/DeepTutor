#!/bin/bash
# Gateway startup script for WeChat iLink dual-bot gateway.
# Starts parent gateway (main) and child gateway (sub) in the same container.
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
INSTALL_DIR="/opt/hermes"

# Activate virtual environment
source "${INSTALL_DIR}/.venv/bin/activate"

# Start child gateway in background
# Unset API_SERVER_KEY so child doesn't start its own API server (parent already has it)
WEIXIN_TOKEN="$CHILD_WEIXIN_TOKEN" \
WEIXIN_ACCOUNT_ID="$CHILD_WEIXIN_ACCOUNT_ID" \
API_SERVER_ENABLED=false \
API_SERVER_KEY="" \
HERMES_HOME=/opt/data/child \
hermes gateway run --accept-hooks &
CHILD_PID=$!
echo "Child gateway started (PID=$CHILD_PID)"

# Start parent gateway in foreground
exec hermes gateway run --accept-hooks
