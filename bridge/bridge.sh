#!/usr/bin/env bash
# feishu_bridge 启动/停止/状态脚本
# 用法:
#   ./bridge.sh start   — 后台启动（已在跑则跳过）
#   ./bridge.sh stop    — 停止
#   ./bridge.sh restart — 重启
#   ./bridge.sh status  — 查看状态
#   ./bridge.sh log     — 跟踪日志

set -euo pipefail

BRIDGE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_PY="$BRIDGE_DIR/.venv/bin/python"
CONFIG="${BRIDGE_CONFIG:-$HOME/.config/opencode/bridge.json}"
LOG_FILE="${BRIDGE_LOG:-/tmp/opencode/bridge.log}"
PID_FILE="${BRIDGE_PID:-/tmp/opencode/bridge.pid}"

mkdir -p "$(dirname "$LOG_FILE")"

is_running() {
  [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null
}

do_start() {
  if is_running; then
    echo "ℹ️ bridge 已在运行 (pid=$(cat "$PID_FILE"))"
    return 0
  fi
  if [ ! -x "$VENV_PY" ]; then
    echo "❌ 未找到虚拟环境: $VENV_PY"
    echo "   先执行: cd $BRIDGE_DIR && python3.11 -m venv .venv && .venv/bin/pip install lark-oapi websockets"
    exit 1
  fi
  if [ ! -f "$CONFIG" ]; then
    echo "❌ 未找到配置文件: $CONFIG（参考 config.example.json）"
    exit 1
  fi
  cd "$BRIDGE_DIR"
  setsid nohup "$VENV_PY" -m feishu_bridge -c "$CONFIG" >> "$LOG_FILE" 2>&1 < /dev/null &
  echo $! > "$PID_FILE"
  # 等待监听就绪（最多 10s）
  for _ in $(seq 1 20); do
    if grep -q "bridge WS 服务已监听" <(tail -5 "$LOG_FILE" 2>/dev/null) && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
      echo "✅ bridge 已启动 (pid=$(cat "$PID_FILE"))，日志: $LOG_FILE"
      return 0
    fi
    sleep 0.5
  done
  if kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
    echo "⚠️ bridge 进程已拉起但未见监听日志，请检查 $LOG_FILE"
  else
    echo "❌ bridge 启动失败，请检查 $LOG_FILE"
    tail -5 "$LOG_FILE"
    exit 1
  fi
}

do_stop() {
  if ! is_running; then
    echo "ℹ️ bridge 未在运行"
    rm -f "$PID_FILE"
    # 兜底清理残留进程
    pkill -f "feishu_bridge -c" 2>/dev/null || true
    return 0
  fi
  local pid
  pid="$(cat "$PID_FILE")"
  echo "⏳ 停止 bridge (pid=$pid)…"
  kill "$pid" 2>/dev/null || true
  for _ in $(seq 1 20); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.5
  done
  if kill -0 "$pid" 2>/dev/null; then
    kill -9 "$pid" 2>/dev/null || true
  fi
  rm -f "$PID_FILE"
  echo "🛑 bridge 已停止"
}

do_status() {
  if is_running; then
    echo "🟢 运行中 (pid=$(cat "$PID_FILE"))"
  else
    echo "⚪ 未运行"
  fi
  # 在线插件实例
  grep "插件已注册" "$LOG_FILE" 2>/dev/null | tail -3 || true
}

case "${1:-status}" in
  start)   do_start ;;
  stop)    do_stop ;;
  restart) do_stop; do_start ;;
  status)  do_status ;;
  log)     tail -f "$LOG_FILE" ;;
  *)       echo "用法: $0 {start|stop|restart|status|log}"; exit 1 ;;
esac
