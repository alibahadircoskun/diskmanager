#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_PATH="${SCRIPT_DIR}/web/app.py"
PYTHON_BIN="${PYTHON_BIN:-/usr/bin/python3}"

if [ "$(id -u)" -ne 0 ]; then
    echo "This script must be run as root." >&2
    exit 1
fi

usage() {
    cat <<'EOF'
Usage: run_web_service.sh [--host HOST] [--port PORT] [--help] [extra args...]

Environment overrides:
  HOST        Defaults to 0.0.0.0
  PORT        Defaults to 8880
  PYTHON_BIN  Defaults to /usr/bin/python3
EOF
}

HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8880}"
EXTRA_ARGS=()

while [ $# -gt 0 ]; do
    case "$1" in
        --host)
            [ $# -ge 2 ] || { echo "Missing value for --host" >&2; exit 1; }
            HOST="$2"
            shift 2
            ;;
        --port)
            [ $# -ge 2 ] || { echo "Missing value for --port" >&2; exit 1; }
            PORT="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        --)
            shift
            EXTRA_ARGS+=("$@")
            break
            ;;
        *)
            EXTRA_ARGS+=("$1")
            shift
            ;;
    esac
done

if ! [[ "$PORT" =~ ^[0-9]+$ ]] || [ "$PORT" -lt 1 ] || [ "$PORT" -gt 65535 ]; then
    echo "Invalid port '${PORT}'. Expected an integer in [1, 65535]." >&2
    exit 1
fi

if [ ! -f "$APP_PATH" ]; then
    echo "Missing Flask app: $APP_PATH" >&2
    exit 1
fi

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
    echo "Python interpreter not found: ${PYTHON_BIN}" >&2
    exit 1
fi

cd "$SCRIPT_DIR"

echo "Starting Disk Manager web app on ${HOST}:${PORT} ..."
exec "$PYTHON_BIN" "$APP_PATH" --host "$HOST" --port "$PORT" "${EXTRA_ARGS[@]}"
