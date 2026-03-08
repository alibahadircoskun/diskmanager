#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ "$(id -u)" -ne 0 ]; then
    echo "This script must be run as root." >&2
    exit 1
fi

cd "$SCRIPT_DIR"

echo "Installing dependencies..."
python3 -m pip install -q -r requirements.txt

PORT="${PORT:-8880}"
echo "Starting Disk Manager web app on port $PORT ..."
exec python3 web/app.py --port "$PORT"
