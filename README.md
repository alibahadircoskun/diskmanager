# diskmanager

Disk management tool for SAS/SATA shelves using fixed PHY paths.

`disk.py` provides:
- Health checks via Hard Disk Sentinel
- Interactive low-level format (`sg_format`)
- Live format progress monitor
- Quick read speed test
- Missing slot detection

## Requirements

- Ubuntu/Debian-like system
- Root access
- Python 3
- `sg3-utils` (`sg_format`, `sg_requests`)
- `smartmontools` (`smartctl`)
- `util-linux` (`lsblk`, `blockdev`)
- Hard Disk Sentinel Linux console binary at `/root/HDSentinel`

## Install

From the repo directory:

```bash
cd /root/diskmanager
sudo bash ./setup.sh
```

Useful setup flags:

```bash
# Enable diskmanager-web at boot
sudo bash ./setup.sh --enable-web

# Replace /etc/default/diskmanager-web from repo defaults
sudo bash ./setup.sh --reset-web-env

# Install CLI + deps only (skip web service install)
sudo bash ./setup.sh --skip-web-service
```

Manual install:

```bash
cd /root/diskmanager
sudo apt-get update
sudo apt-get install -y python3 python3-pip sg3-utils smartmontools util-linux unzip wget ca-certificates
sudo python3 -m pip install -r ./requirements.txt || sudo python3 -m pip install --break-system-packages -r ./requirements.txt
```

Download HDSentinel:

```bash
cd /tmp
wget -q "https://www.hdsentinel.com/hdslin/hdsentinel-020c-x64.zip" -O hdsentinel.zip
unzip -o hdsentinel.zip -d hdsentinel_tmp
sudo mv hdsentinel_tmp/HDSentinel /root/HDSentinel
sudo chmod +x /root/HDSentinel
rm -rf hdsentinel.zip hdsentinel_tmp
```

Make command available globally:

```bash
sudo chmod +x ./disk.py ./install_web_service.sh ./run_web_service.sh
sudo ln -sf "$(pwd)/disk.py" /usr/local/bin/disk
```

## Usage

Run menu:

```bash
sudo disk
```

Or run subcommands directly:

```bash
sudo disk health
sudo disk health --dump
sudo disk health --raw
sudo disk format
sudo disk progress
sudo disk speedtest
sudo disk missing
```

## Web UI as a Service (survives shell exit)

`setup.sh` installs dependencies, installs/updates the `diskmanager-web` systemd service,
and restarts it (unless `--skip-web-service` is used).

Install/update only the web service with one command:

```bash
cd /root/diskmanager
sudo bash ./install_web_service.sh
```

Optional install flags:

```bash
# Enable at boot
sudo bash ./install_web_service.sh --enable

# Disable at boot
sudo bash ./install_web_service.sh --disable

# Replace /etc/default/diskmanager-web from repo defaults
sudo bash ./install_web_service.sh --reset-env

# Update files without restarting the service
sudo bash ./install_web_service.sh --no-restart
```

`install_web_service.sh` also rewrites `WorkingDirectory` and `ExecStart` in the
installed unit so the service points to the current repo location.

Start/stop/restart/status:

```bash
sudo systemctl start diskmanager-web
sudo systemctl stop diskmanager-web
sudo systemctl restart diskmanager-web
sudo systemctl status diskmanager-web
```

View live logs:

```bash
sudo journalctl -u diskmanager-web -f
```

Run the web app directly (without systemd):

```bash
cd /root/diskmanager
sudo bash ./run_web_service.sh
```

Override bind address/port (either env vars or args):

```bash
sudo env HOST=127.0.0.1 PORT=8890 bash ./run_web_service.sh
sudo bash ./run_web_service.sh --host 127.0.0.1 --port 8890
```

Optional boot autostart (disabled by default):

```bash
sudo systemctl enable diskmanager-web
```

Verify it survives shell close:

```bash
sudo systemctl start diskmanager-web
exit
# reconnect later
sudo systemctl status diskmanager-web
```

## Web UI Slot Command Center

- Bays support multi-select.
- Click a bay to toggle it in/out of selection.
- Slots table rows also support multi-select via click/checkbox, and each row has direct `Health`, `Speed`, `Format`, and `Monitor` action buttons.
- Quick selection shortcuts are available: `Select Present`, `Invert`, and `Clear Selection`.
- Keyboard shortcuts: `R` refresh, `H` show health, `P` select present, `I` invert, `C` clear selection, `A` toggle table select-all.
- Table checkbox selection supports Shift-click range select.
- The most recently selected bay is used as the primary detail slot.
- Selection summary always shows selected vs actionable counts.
- `Health`, `Speed Test`, `Format`, and `Monitor` actions only run on selected `PRESENT` disks.
- If no disks are present, the status line shows slot-missing state and actions stay disabled with concise reason text.
- Monitor polling is batched through one `POST /api/format/poll` request per tick for active paths.
- Monitor auto-stops when all monitored selected paths reach terminal states (`done`/`failed`/`missing`).

## Web UI Logs Tab

- Top navigation includes `Slots` and `Logs`.
- `Logs` loads on first tab open and refreshes only when `Refresh Logs` is clicked.
- The viewer shows the last 500 lines (oldest-to-newest within that window).
- The source path is runtime-resolved from `disk.LOGFILE`, and the UI shows the absolute source path plus last refresh timestamp.

## Command Notes

- `health`
  - Uses PHY symlink paths (`/dev/disk/by-path/...-phy...`) for detection.
  - Shows live scan progress with elapsed time and scanned device count.
  - Prints slot-aligned health table for detected disks.
- `health --dump`
  - Includes full HDSentinel dump after the table.
  - Annotates dump device lines with slot numbers where possible.
- `health --raw`
  - Prints raw HDSentinel output directly (legacy behavior).
- `missing`
  - Shows all expected slots (sorted from slot `0` upward) and marks `PRESENT` or `MISSING`.
  - Includes `Device`, `Serial`, and `Model` columns.
- `format`
  - Interactive destructive format flow using `sg_format`.
  - Requires typing `YES` to confirm.
- `progress`
  - Live monitor for `sg_format` state across selected disks.
- `speedtest`
  - Runs direct read test (`dd iflag=direct`) and prints per-disk read speed.

## Safety

- Run as `root`.
- `format` permanently erases disk data.
- Verify selected devices and slots before confirming.

## Log

Operations are logged next to `disk.py` in the install directory:

```text
<install-dir>/diskops.log
```

Check the active runtime log path:

```bash
cd <install-dir>
python3 -c 'from pathlib import Path; import disk; print(Path(disk.LOGFILE).resolve())'
```

Legacy hardcoded locations like `/var/log/diskops.log` are no longer used.
