#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: install_web_service.sh [--enable] [--disable] [--reset-env] [--no-restart] [--help]

Install/update the diskmanager-web systemd unit and environment file.

Options:
  --enable      Enable service at boot.
  --disable     Disable service at boot.
  --reset-env   Replace /etc/default/diskmanager-web from repo defaults.
  --no-restart  Skip service restart after install/update.
  --help        Show this help text.
EOF
}

if [ "$(id -u)" -ne 0 ]; then
    echo "This script must be run as root." >&2
    exit 1
fi

if ! command -v systemctl >/dev/null 2>&1; then
    echo "systemctl not found. This script requires a systemd-based host." >&2
    exit 1
fi

if [ ! -d /run/systemd/system ]; then
    echo "systemd is not PID 1 on this host; refusing to install service." >&2
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_SRC="${SCRIPT_DIR}/systemd/diskmanager-web.service"
ENV_SRC="${SCRIPT_DIR}/systemd/diskmanager-web.env"
RUNNER_SRC="${SCRIPT_DIR}/run_web_service.sh"
SERVICE_DST="/etc/systemd/system/diskmanager-web.service"
ENV_DST="/etc/default/diskmanager-web"
SERVICE_NAME="diskmanager-web"

ENABLE=0
DISABLE=0
RESET_ENV=0
RESTART=1

for arg in "$@"; do
    case "$arg" in
        --enable)
            ENABLE=1
            ;;
        --disable)
            DISABLE=1
            ;;
        --reset-env)
            RESET_ENV=1
            ;;
        --no-restart)
            RESTART=0
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $arg" >&2
            usage >&2
            exit 1
            ;;
    esac
done

if [ "$ENABLE" -eq 1 ] && [ "$DISABLE" -eq 1 ]; then
    echo "Use either --enable or --disable, not both." >&2
    exit 1
fi

if [ ! -f "$SERVICE_SRC" ]; then
    echo "Missing service source file: $SERVICE_SRC" >&2
    exit 1
fi
if [ ! -f "$ENV_SRC" ]; then
    echo "Missing env source file: $ENV_SRC" >&2
    exit 1
fi
if [ ! -f "$RUNNER_SRC" ]; then
    echo "Missing runner script: $RUNNER_SRC" >&2
    exit 1
fi

# Validate runner + Python deps before writing systemd config.
if ! bash "$RUNNER_SRC" --help >/dev/null 2>&1; then
    echo "Web runner preflight failed: ${RUNNER_SRC} --help" >&2
    echo "Install web dependencies first (run ./setup.sh or pip install -r requirements.txt)." >&2
    exit 1
fi

chmod +x "$RUNNER_SRC"

escaped_script_dir="${SCRIPT_DIR//&/\\&}"
escaped_runner="${RUNNER_SRC//&/\\&}"
tmp_service="$(mktemp)"
trap 'rm -f "$tmp_service"' EXIT
sed \
    -e "s|^WorkingDirectory=.*|WorkingDirectory=${escaped_script_dir}|" \
    -e "s|^ExecStart=.*|ExecStart=${escaped_runner}|" \
    "$SERVICE_SRC" > "$tmp_service"
install -m 0644 "$tmp_service" "$SERVICE_DST"

if [ ! -f "$ENV_DST" ] || [ "$RESET_ENV" -eq 1 ]; then
    install -m 0644 "$ENV_SRC" "$ENV_DST"
    echo "Installed environment file to ${ENV_DST}."
else
    echo "Keeping existing ${ENV_DST} (use --reset-env to replace it)."
fi

systemctl daemon-reload

if [ "$RESTART" -eq 1 ]; then
    systemctl restart "$SERVICE_NAME"
else
    echo "Skipping service restart (--no-restart)."
fi

if [ "$ENABLE" -eq 1 ]; then
    systemctl enable "$SERVICE_NAME"
fi
if [ "$DISABLE" -eq 1 ]; then
    systemctl disable "$SERVICE_NAME"
fi

active_state="$(systemctl is-active "$SERVICE_NAME" 2>/dev/null || true)"
enabled_state="$(systemctl is-enabled "$SERVICE_NAME" 2>/dev/null || true)"
echo "Service state: active=${active_state:-unknown}, enabled=${enabled_state:-unknown}"
echo
echo "${SERVICE_NAME} service status:"
systemctl status "$SERVICE_NAME" --no-pager | sed -n '1,12p' || true
