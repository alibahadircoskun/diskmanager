#!/usr/bin/env bash
set -euo pipefail

# Must run as root
if [ "$(id -u)" -ne 0 ]; then
    echo "This script must be run as root."
    exit 1
fi

REPO_DIR="/root/diskmanager"
DISK_PY="${REPO_DIR}/disk.py"

echo "Installing dependencies for disk.py..."

apt-get update -qq
apt-get install -y -qq \
    ca-certificates \
    python3 \
    sg3-utils \
    smartmontools \
    util-linux \
    unzip \
    wget

# HDSentinel
if [ ! -x /root/HDSentinel ]; then
    echo "Downloading HDSentinel..."

    cd /tmp
    wget -q "https://www.hdsentinel.com/hdslin/hdsentinel-020c-x64.zip" -O hdsentinel.zip
    unzip -o hdsentinel.zip -d hdsentinel_tmp
    mv hdsentinel_tmp/HDSentinel /root/HDSentinel
    chmod +x /root/HDSentinel
    rm -rf hdsentinel.zip hdsentinel_tmp
    echo "HDSentinel installed to /root/HDSentinel"
else
    echo "HDSentinel already installed, skipping."
fi

if [ ! -f "${DISK_PY}" ]; then
    echo "Expected script not found: ${DISK_PY}"
    echo "Clone/copy the repo to /root/diskmanager first."
    exit 1
fi

# Use repo copy as single source of truth.
chmod +x "${DISK_PY}"
ln -sf "${DISK_PY}" /usr/local/bin/disk

echo "Done. Run 'disk' to start."
