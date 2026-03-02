#!/usr/bin/env bash
set -euo pipefail

# Must run as root
if [ "$(id -u)" -ne 0 ]; then
    echo "This script must be run as root."
    exit 1
fi

# Resolve the directory this script lives in so disk.py is found
# regardless of where the repo was cloned
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

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

# Make disk.py runnable as 'disk' from anywhere
chmod +x "${SCRIPT_DIR}/disk.py"
ln -sf "${SCRIPT_DIR}/disk.py" /usr/local/bin/disk

echo "Done. Run 'disk' to start."
