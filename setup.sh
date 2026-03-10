#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: setup.sh [--enable-web] [--reset-web-env] [--skip-web-service] [--help]

Installs CLI/web dependencies, installs HDSentinel (if missing),
links `disk` to /usr/local/bin, installs pinned web Python deps from
requirements.txt, and optionally installs the systemd web service.

Options:
  --enable-web       Enable diskmanager-web service at boot.
  --reset-web-env    Replace /etc/default/diskmanager-web from repo defaults.
  --skip-web-service Skip systemd web service install step.
  --help             Show this help text.
EOF
}

# Must run as root.
if [ "$(id -u)" -ne 0 ]; then
    echo "This script must be run as root." >&2
    exit 1
fi

ENABLE_WEB=0
RESET_WEB_ENV=0
SKIP_WEB_SERVICE=0

for arg in "$@"; do
    case "$arg" in
        --enable-web)
            ENABLE_WEB=1
            ;;
        --reset-web-env)
            RESET_WEB_ENV=1
            ;;
        --skip-web-service)
            SKIP_WEB_SERVICE=1
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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="${SCRIPT_DIR}"
DISK_PY="${REPO_DIR}/disk.py"
REQ_FILE="${REPO_DIR}/requirements.txt"
INSTALL_WEB_SH="${REPO_DIR}/install_web_service.sh"
RUN_WEB_SH="${REPO_DIR}/run_web_service.sh"
HDSENTINEL_BIN="/root/HDSentinel"
HDSENTINEL_URL="https://www.hdsentinel.com/hdslin/hdsentinel-020c-x64.zip"

if [ ! -f "${DISK_PY}" ]; then
    echo "Expected script not found: ${DISK_PY}" >&2
    echo "Run setup.sh from inside the diskmanager repo." >&2
    exit 1
fi

echo "Installing system dependencies for disk CLI/web..."

apt-get update -qq
apt-get install -y -qq \
    ca-certificates \
    python3 \
    python3-pip \
    sg3-utils \
    smartmontools \
    util-linux \
    unzip \
    wget

# HDSentinel
if [ ! -x "${HDSENTINEL_BIN}" ]; then
    echo "Downloading HDSentinel..."

    tmp_dir="$(mktemp -d)"
    zip_path="${tmp_dir}/hdsentinel.zip"
    unpack_dir="${tmp_dir}/hdsentinel_tmp"

    wget -q "${HDSENTINEL_URL}" -O "${zip_path}"
    unzip -o "${zip_path}" -d "${unpack_dir}"

    if [ ! -f "${unpack_dir}/HDSentinel" ]; then
        rm -rf "${tmp_dir}"
        echo "Downloaded archive did not contain HDSentinel binary." >&2
        exit 1
    fi

    install -m 0755 "${unpack_dir}/HDSentinel" "${HDSENTINEL_BIN}"
    rm -rf "${tmp_dir}"
    echo "HDSentinel installed to ${HDSENTINEL_BIN}"
else
    echo "HDSentinel already installed, skipping."
fi

# Use repo copy as single source of truth.
chmod +x "${DISK_PY}"
if [ -f "${INSTALL_WEB_SH}" ]; then
    chmod +x "${INSTALL_WEB_SH}"
fi
if [ -f "${RUN_WEB_SH}" ]; then
    chmod +x "${RUN_WEB_SH}"
fi
ln -sf "${DISK_PY}" /usr/local/bin/disk

if [ -f "${REQ_FILE}" ]; then
    echo "Installing pinned Python dependencies for web UI from ${REQ_FILE}..."
    if ! python3 -m pip install -q -r "${REQ_FILE}"; then
        echo "Retrying pip install with --break-system-packages ..."
        python3 -m pip install -q --break-system-packages -r "${REQ_FILE}"
    fi
else
    echo "Warning: ${REQ_FILE} not found. Skipping pip install."
fi

if [ "${SKIP_WEB_SERVICE}" -eq 1 ]; then
    echo "Skipping web service install (--skip-web-service)."
elif [ -f "${INSTALL_WEB_SH}" ] && command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; then
    echo "Installing/updating diskmanager-web service..."
    web_args=()
    if [ "${ENABLE_WEB}" -eq 1 ]; then
        web_args+=(--enable)
    fi
    if [ "${RESET_WEB_ENV}" -eq 1 ]; then
        web_args+=(--reset-env)
    fi
    bash "${INSTALL_WEB_SH}" "${web_args[@]}"
else
    echo "Warning: service installer or active systemd missing. Skipping web service install."
fi

echo "Done."
echo "- Run 'disk' for CLI operations."
echo "- Use 'systemctl status diskmanager-web' for web UI service status."
echo "- Optional boot autostart: systemctl enable diskmanager-web"
