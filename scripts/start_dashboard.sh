#!/bin/bash
# 看板启动脚本：kill 残留 → 启动 → 轮询等待就绪
# 用法: bash scripts/start_dashboard.sh [port]

PORT=${1:-9080}
VENV_PYTHON=".venv/bin/python"
MAX_WAIT=60
INTERVAL=2

# 1. kill 残留进程
PID=$(lsof -ti:$PORT 2>/dev/null)
if [ -n "$PID" ]; then
  echo "[dashboard] killing existing process on port $PORT (PID: $PID)"
  kill $PID 2>/dev/null
  sleep 1
  # 确认已杀死
  PID2=$(lsof -ti:$PORT 2>/dev/null)
  if [ -n "$PID2" ]; then
    echo "[dashboard] force killing PID $PID2"
    kill -9 $PID2 2>/dev/null
    sleep 1
  fi
fi

# 2. 后台启动看板
echo "[dashboard] starting on port $PORT ..."
$VENV_PYTHON -m aivectormemory web --port $PORT 2>&1 &
BG_PID=$!
echo "[dashboard] background PID: $BG_PID"

# 3. 轮询等待端口就绪
WAITED=0
while [ $WAITED -lt $MAX_WAIT ]; do
  # 检查进程是否还活着
  if ! kill -0 $BG_PID 2>/dev/null; then
    echo "[dashboard] ERROR: process $BG_PID exited unexpectedly"
    exit 1
  fi
  # 检查端口是否已绑定
  if lsof -ti:$PORT >/dev/null 2>&1; then
    echo "[dashboard] ready! (waited ${WAITED}s)"
    echo "[dashboard] http://127.0.0.1:$PORT"
    exit 0
  fi
  sleep $INTERVAL
  WAITED=$((WAITED + INTERVAL))
done

echo "[dashboard] ERROR: timeout after ${MAX_WAIT}s, port $PORT not ready"
kill $BG_PID 2>/dev/null
exit 1
