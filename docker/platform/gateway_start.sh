#!/bin/bash
# Hermes Gateway startup script — parent + child dual-bot launcher
set -e

HERMES_HOME="${HERMES_HOME:-/opt/data}"
PARENT_PID=""
CHILD_PID=""

cleanup() {
    echo "[gateway_start] Shutting down..."
    [ -n "$PARENT_PID" ] && kill "$PARENT_PID" 2>/dev/null || true
    [ -n "$CHILD_PID" ] && kill "$CHILD_PID" 2>/dev/null || true
    wait
    echo "[gateway_start] Shutdown complete."
}
trap cleanup SIGTERM SIGINT

# Wait for Docker network to stabilize (resolve critical hosts)
echo "[gateway_start] Waiting for Docker network to stabilize..."
for host in rkllama deeptutor; do
    for i in $(seq 1 30); do
        if getent hosts "$host" >/dev/null 2>&1; then
            break
        fi
        sleep 1
    done
done

# Check iLink server connectivity
echo "[gateway_start] Checking iLink server reachability..."
for i in $(seq 1 5); do
    if curl -sf --connect-timeout 5 "https://ilinkai.weixin.qq.com" >/dev/null 2>&1; then
        echo "[gateway_start] iLink server reachable (attempt $i)"
        break
    fi
    echo "[gateway_start] iLink server not reachable yet (attempt $i), retrying..."
    sleep 2
done

# Start parent gateway
echo "[gateway_start] Starting parent gateway..."
hermes gateway run &
PARENT_PID=$!
echo "[gateway_start] Parent gateway PID $PARENT_PID"

# Wait for parent to initialize
echo "[gateway_start] Waiting 5s, then starting child gateway..."
sleep 5

# Start child gateway with separate HERMES_HOME + child-specific WeChat identity
if [ -d "${HERMES_HOME}/child" ]; then
    echo "[gateway_start] Starting child gateway..."
    WEIXIN_ACCOUNT_ID="${CHILD_WEIXIN_ACCOUNT_ID:-9fd8e4a28c1d@im.bot}" \
    WEIXIN_TOKEN="${CHILD_WEIXIN_TOKEN}" \
    API_SERVER_ENABLED=false \
    API_SERVER_KEY="" \
    HERMES_HOME="${HERMES_HOME}/child" hermes gateway run &
    CHILD_PID=$!
    echo "[gateway_start] Child gateway PID $CHILD_PID"
fi

echo "[gateway_start] Both running (parent=$PARENT_PID child=$CHILD_PID)"

# Wait for either to exit
wait
