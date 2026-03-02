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

If you already have `setup.sh` on the machine:

```bash
sudo bash /root/setup.sh
```

Manual install:

```bash
sudo apt-get update
sudo apt-get install -y python3 sg3-utils smartmontools util-linux unzip wget ca-certificates
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
sudo chmod +x ./disk.py
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

Operations are logged to:

```text
/var/log/diskops.log
```
