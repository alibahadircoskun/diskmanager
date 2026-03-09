#!/usr/bin/env python3
"""disk.py — disk health, format, and progress monitoring tool."""

from __future__ import annotations

import argparse
import concurrent.futures
import logging
import logging.handlers
import os
import pty
import re
import select
import shutil
import signal
import subprocess
import sys
import termios
import time
import tty
from pathlib import Path

PORTS = [
    "/dev/disk/by-path/pci-0000:02:00.0-sas-exp0x500065b36789abff-phy6-lun-0",
    "/dev/disk/by-path/pci-0000:02:00.0-sas-exp0x500065b36789abff-phy4-lun-0",
    "/dev/disk/by-path/pci-0000:02:00.0-sas-exp0x500065b36789abff-phy11-lun-0",
    "/dev/disk/by-path/pci-0000:02:00.0-sas-exp0x500065b36789abff-phy9-lun-0",
    "/dev/disk/by-path/pci-0000:02:00.0-sas-exp0x500065b36789abff-phy5-lun-0",
    "/dev/disk/by-path/pci-0000:02:00.0-sas-exp0x500065b36789abff-phy3-lun-0",
    "/dev/disk/by-path/pci-0000:02:00.0-sas-exp0x500065b36789abff-phy10-lun-0",
    "/dev/disk/by-path/pci-0000:02:00.0-sas-exp0x500065b36789abff-phy8-lun-0",
    "/dev/disk/by-path/pci-0000:02:00.0-sas-exp0x500065b36789abff-phy2-lun-0",
    "/dev/disk/by-path/pci-0000:02:00.0-sas-exp0x500065b36789abff-phy7-lun-0",
    "/dev/disk/by-path/pci-0000:02:00.0-sas-exp0x500065b36789abff-phy16-lun-0",
    "/dev/disk/by-path/pci-0000:02:00.0-sas-exp0x500065b36789abff-phy17-lun-0",
]

SLOTS = [8, 5, 2, 11, 1, 4, 7, 10, 0, 3, 6, 9]
PORT_TO_SLOT = dict(zip(PORTS, SLOTS))

HDSENTINEL = "/root/HDSentinel"
LOGFILE    = str(Path(__file__).resolve().parent / "diskops.log")

_log = logging.getLogger("diskops")
_log.setLevel(logging.INFO)
try:
    _handler = logging.handlers.RotatingFileHandler(
        LOGFILE, maxBytes=5 * 1024 * 1024, backupCount=3
    )
    _handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    _log.addHandler(_handler)
except PermissionError:
    pass

def log(msg: str) -> None:
    _log.info(msg)

_TTY = sys.stdout.isatty()

def _a(code: str, t: str) -> str:
    return f"\033[{code}m{t}\033[0m" if _TTY else t

def bold(t: str) -> str: return _a("1",    t)
def dim(t: str)  -> str: return _a("2",    t)
def grn(t: str)  -> str: return _a("32",   t)
def ylw(t: str)  -> str: return _a("33",   t)
def cyn(t: str)  -> str: return _a("36",   t)
def wht(t: str)  -> str: return _a("37",   t)
def red(t: str)  -> str: return _a("31",   t)
def bred(t: str) -> str: return _a("1;31", t)
def bgrn(t: str) -> str: return _a("1;32", t)
def bylw(t: str) -> str: return _a("1;33", t)
def bcyn(t: str) -> str: return _a("1;36", t)

def prompt_input(prompt: str) -> str:
    """Read a line robustly from TTY, handling both common Backspace keycodes."""
    if not sys.stdin.isatty() or not sys.stdout.isatty():
        return input(bold(prompt))

    fd = sys.stdin.fileno()
    old = termios.tcgetattr(fd)
    buf: list[str] = []
    try:
        tty.setraw(fd)
        sys.stdout.write(bold(prompt))
        sys.stdout.flush()
        while True:
            ch = os.read(fd, 1)
            if ch in (b"\r", b"\n"):
                sys.stdout.write("\r\n")
                sys.stdout.flush()
                break
            if ch in (b"\x7f", b"\x08"):  # DEL or Ctrl+H
                if buf:
                    buf.pop()
                    sys.stdout.write("\b \b")
                    sys.stdout.flush()
                continue
            if ch == b"\x03":
                raise KeyboardInterrupt
            if ch == b"\x04":
                break
            try:
                c = ch.decode("utf-8")
            except UnicodeDecodeError:
                continue
            if c.isprintable():
                buf.append(c)
                sys.stdout.write(c)
                sys.stdout.flush()
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old)
    return "".join(buf)

def require_root() -> None:
    if os.geteuid() != 0:
        sys.exit("This script must be run as root.")

def fmt_duration(secs: float) -> str:
    secs = max(0, int(secs))
    h, rem = divmod(secs, 3600)
    m, s   = divmod(rem, 60)
    if h:   return f"{h}h {m:02d}m {s:02d}s"
    elif m: return f"{m}m {s:02d}s"
    else:   return f"{s}s"

def prep_for_format(dev_path: str) -> None:
    name = Path(dev_path).name
    sysfs = f"/sys/block/{name}/device"
    try:
        Path(f"{sysfs}/queue_depth").write_text("1")
    except OSError as e:
        _log.warning("Failed to set queue_depth for %s: %s", dev_path, e)
    try:
        Path(f"{sysfs}/timeout").write_text("5")
    except OSError as e:
        _log.warning("Failed to set timeout for %s: %s", dev_path, e)

def sg_device(dev_path: str) -> str | None:
    name = Path(dev_path).name
    sg_dir = Path(f"/sys/block/{name}/device/scsi_generic")
    try:
        sg_name = next(sg_dir.iterdir()).name
        return f"/dev/{sg_name}"
    except (StopIteration, OSError):
        return None

class Device:
    def __init__(self, path: str, port: str):
        self.path       = path
        self.port       = port
        self.slot       = PORT_TO_SLOT.get(port, "?")
        self.model      = ""
        self.serial     = ""
        self.health     = "?"
        self.size       = ""
        self.fmt_status = ""  # logical block size in bytes, e.g. "512", "520", "528"
        self.zeroed     = ""  # "zero" if disk is zeroed, "data" if not, "" if unknown

    def __str__(self) -> str:
        return self.path

def discover() -> list[Device]:
    seen: set[str] = set()
    devices: list[Device] = []
    for port in PORTS:
        p = Path(port)
        if p.is_block_device():
            real = str(p.resolve())
            if real not in seen:
                seen.add(real)
                devices.append(Device(real, port))
    devices.sort(key=lambda d: (d.slot if isinstance(d.slot, int) else 999))
    return devices

def enrich_lsblk(devices: list[Device]) -> None:
    paths = [d.path for d in devices]
    r = subprocess.run(
        ["lsblk", "-dn", "-P", "-o", "NAME,SIZE,SERIAL,MODEL"] + paths,
        capture_output=True, text=True, timeout=30,
    )
    info: dict[str, tuple[str, str, str]] = {}
    for line in r.stdout.splitlines():
        kv = dict(m.group(1, 2) for m in re.finditer(r'(\w+)="([^"]*)"', line))
        name = kv.get("NAME", "")
        if name:
            info[f"/dev/{name}"] = (kv.get("SIZE", ""), kv.get("SERIAL", ""), kv.get("MODEL", ""))
    for dev in devices:
        dev.size, dev.serial, dev.model = info.get(dev.path, ("", "", ""))
    # Fill missing serials via smartctl
    if shutil.which("smartctl"):
        for dev in devices:
            if not dev.serial:
                r2 = subprocess.run(["smartctl", "-i", "-d", "scsi", dev.path],
                                    capture_output=True, text=True, timeout=30)
                m = re.search(r"Serial number:\s*(\S+)", r2.stdout, re.I)
                if m:
                    dev.serial = m.group(1)

def _zero_sample_positions(total_bytes: int, sample_size: int = 4096, sample_count: int = 5) -> list[int]:
    """Return evenly distributed sample start offsets across the device."""
    if total_bytes <= 0 or sample_size <= 0 or sample_count <= 0:
        return []
    max_start = max(0, total_bytes - sample_size)
    if sample_count == 1:
        return [0]
    return [(max_start * i) // (sample_count - 1) for i in range(sample_count)]

def _is_disk_zeroed(dev_path: str, sample_count: int = 5) -> str:
    """Sample a disk to check if it's been zeroed. Returns 'zero', 'data', or ''."""
    try:
        # Get disk size
        r = subprocess.run(["blockdev", "--getsize64", dev_path],
                          capture_output=True, text=True, timeout=5)
        if r.returncode != 0:
            return ""
        total_bytes = int(r.stdout.strip())
        if total_bytes == 0:
            return ""

        sample_size = 4096  # 4KB per sample
        positions = _zero_sample_positions(total_bytes, sample_size=sample_size, sample_count=sample_count)
        if not positions:
            return ""

        total_samples = len(positions)
        # Require enough successful reads to avoid false "zero" on sparse failures.
        # ceil(0.6 * n) computed as ceil((3*n)/5) -> (3*n + 4) // 5.
        min_success = max(3, ((3 * total_samples) + 4) // 5)
        successful_reads = 0

        for pos in positions:
            try:
                r = subprocess.run(
                    ["dd", f"if={dev_path}", "of=/dev/stdout", f"skip={pos // 512}",
                     "bs=512", "count=8", "iflag=direct"],
                    stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=10
                )
                if r.returncode != 0 or not r.stdout:
                    continue
                successful_reads += 1
                # Any non-zero sample means this disk is not fully zeroed.
                if any(b != 0 for b in r.stdout):
                    return "data"
            except (subprocess.TimeoutExpired, OSError):
                continue

        if successful_reads < min_success:
            return ""
        return "zero"
    except (ValueError, OSError, subprocess.TimeoutExpired):
        return ""

def enrich_fmt_status(devices: list[Device]) -> None:
    """Populate fmt_status using sg_readcap to read the logical block size."""
    if not shutil.which("sg_readcap"):
        return

    def _fetch(dev: Device) -> None:
        try:
            r = subprocess.run(["sg_readcap", dev.path],
                               capture_output=True, text=True, timeout=30)
            m = re.search(r"Logical block length=(\d+)\s*bytes", r.stdout + r.stderr, re.I)
            if m:
                dev.fmt_status = m.group(1)
        except (subprocess.TimeoutExpired, OSError):
            pass

    with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(devices), len(SLOTS))) as ex:
        list(ex.map(_fetch, devices))

def enrich_zeroed_status(devices: list[Device]) -> None:
    """Detect if disks have been zeroed by sampling sectors."""
    def _fetch(dev: Device) -> None:
        dev.zeroed = _is_disk_zeroed(dev.path)

    with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(devices), len(SLOTS))) as ex:
        list(ex.map(_fetch, devices))

def _fmt_status_color(status: str, zeroed: str = "") -> str:
    base = ""
    if status == "512":
        base = "512B"
    elif status:
        base = f"{status}B"
    else:
        base = "?"

    if zeroed == "zero":
        return bgrn(f"{base}✓")
    elif zeroed == "data":
        return ylw(f"{base}✗")
    elif status:
        return bgrn(base) if status == "512" else ylw(base)
    return dim(base)

def _health_color(health_str: str) -> str:
    try:
        val = float(health_str)
        fn  = bgrn if val >= 90 else bred
        return fn(f"{health_str}%")
    except (ValueError, TypeError):
        return dim(health_str)

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")

def _visible_len(text: str) -> int:
    return len(_ANSI_RE.sub("", text))

def _pad(text: str, width: int) -> str:
    return text + (" " * max(0, width - _visible_len(text)))

def print_table(devices: list[Device], show_health: bool = True) -> None:
    idx_w, dev_w, size_w, fmt_w, hlth_w, ser_w, mdl_w, slot_w = 4, 12, 8, 5, 8, 22, 20, 4
    print()
    if show_health:
        print("  " + _pad(bold("#"), idx_w) + " " + _pad(bold("Device"), dev_w) + "  " +
              _pad(bold("Size"), size_w) + "  " + _pad(bold("Fmt"), fmt_w) + "  " +
              _pad(bold("Health"), hlth_w) + "  " + _pad(bold("Serial"), ser_w) + "  " +
              _pad(bold("Model"), mdl_w) + "  " + _pad(bold("Slot"), slot_w))
        print(f"  {'─'*idx_w} {'─'*dev_w}  {'─'*size_w}  {'─'*fmt_w}  {'─'*hlth_w}  {'─'*ser_w}  {'─'*mdl_w}  {'─'*slot_w}")
        for i, d in enumerate(devices, 1):
            h_fld    = _health_color(d.health) if d.health not in ("?", "--") else dim(d.health)
            fmt_fld  = _fmt_status_color(d.fmt_status, d.zeroed)
            slot_fld = cyn(str(d.slot))
            print("  " + _pad(f"[{i:<2}]", idx_w) + " " + _pad(d.path, dev_w) + "  " +
                  _pad(d.size, size_w) + "  " + _pad(fmt_fld, fmt_w) + "  " +
                  _pad(h_fld, hlth_w) + "  " +
                  _pad(d.serial or "N/A", ser_w) + "  " + _pad(d.model or "unknown", mdl_w) + "  " +
                  _pad(slot_fld, slot_w))
    else:
        print("  " + _pad(bold("#"), idx_w) + " " + _pad(bold("Device"), dev_w) + "  " +
              _pad(bold("Size"), size_w) + "  " + _pad(bold("Fmt"), fmt_w) + "  " +
              _pad(bold("Serial"), ser_w) + "  " + _pad(bold("Model"), mdl_w) + "  " +
              _pad(bold("Slot"), slot_w))
        print(f"  {'─'*idx_w} {'─'*dev_w}  {'─'*size_w}  {'─'*fmt_w}  {'─'*ser_w}  {'─'*mdl_w}  {'─'*slot_w}")
        for i, d in enumerate(devices, 1):
            fmt_fld  = _fmt_status_color(d.fmt_status, d.zeroed)
            slot_fld = cyn(str(d.slot))
            print("  " + _pad(f"[{i:<2}]", idx_w) + " " + _pad(d.path, dev_w) + "  " +
                  _pad(d.size, size_w) + "  " + _pad(fmt_fld, fmt_w) + "  " +
                  _pad(d.serial or "N/A", ser_w) + "  " + _pad(d.model or "unknown", mdl_w) + "  " +
                  _pad(slot_fld, slot_w))
    print()

def _run_with_progress(cmd: list[str], label: str, timeout: int = 300) -> tuple[int, str, str]:
    start = time.monotonic()
    if not _TTY:
        print(f"  {cyn(label)}", flush=True)

    master_fd, slave_fd = pty.openpty()
    p = subprocess.Popen(cmd, stdout=slave_fd, stderr=slave_fd, close_fds=True)
    os.close(slave_fd)
    os.set_blocking(master_fd, False)

    spins = "|/-\\"
    i = 0
    scanned = 0
    out_chunks: list[bytes] = []
    timed_out = False

    while True:
        rc = p.poll()
        ready, _, _ = select.select([master_fd], [], [], 0.2)
        for _ in ready:
            try:
                chunk = os.read(master_fd, 65536)
            except BlockingIOError:
                chunk = b""
            except OSError:
                chunk = b""
            if not chunk:
                continue
            out_chunks.append(chunk)

        if out_chunks:
            cur = b"".join(out_chunks).decode("utf-8", errors="replace")
            ids = {m.group(1) for m in re.finditer(r"(?:^|\r)Device\s+(\d+)\s*:", cur)}
            scanned = len(ids)

        elapsed = time.monotonic() - start
        if _TTY and rc is None:
            dev_txt = f"{scanned} dev scanned"
            sys.stdout.write(f"\r  {cyn(label)} {spins[i % len(spins)]} {int(elapsed)}s  {dev_txt}")
            sys.stdout.flush()
            i += 1

        if rc is not None:
            break

        if elapsed >= timeout:
            timed_out = True
            p.kill()
            p.wait()
            break

    # Drain any remaining PTY bytes after process exit.
    while True:
        try:
            chunk = os.read(master_fd, 65536)
        except (BlockingIOError, OSError):
            break
        if not chunk:
            break
        out_chunks.append(chunk)
    os.close(master_fd)

    if timed_out:
        if _TTY:
            sys.stdout.write(f"\r  {bred(label + ' TIMED OUT after ' + fmt_duration(timeout))}{' ' * 8}\n")
            sys.stdout.flush()
        else:
            print(f"  {bred(label + ' TIMED OUT after ' + fmt_duration(timeout))}")
        return 1, b"".join(out_chunks).decode("utf-8", errors="replace"), ""

    if _TTY:
        dev_txt = f"{scanned} dev scanned"
        sys.stdout.write(f"\r  {cyn(label)} done in {fmt_duration(time.monotonic() - start)}  {dev_txt}{' ' * 8}\n")
        sys.stdout.flush()
    else:
        print(f"  {dim(f'done in {fmt_duration(time.monotonic() - start)}')}")

    out = b"".join(out_chunks).decode("utf-8", errors="replace")
    err = ""
    return p.returncode or 0, out, err

def _slot_for_path(path: str, devices: list[Device]) -> int | str:
    for d in devices:
        if path in (d.port, d.path):
            return d.slot
    return "?"

def _parse_hdsentinel_health(output: str, devices: list[Device]) -> None:
    """Populate per-device health from HDSentinel text output."""
    by_key: dict[str, Device] = {}
    for d in devices:
        by_key[d.port] = d
        by_key[d.path] = d

    pat = re.compile(
        r"(?ms)^HDD Device\s+\d+:\s+(?P<path>\S+)\n(?P<body>.*?)(?=^HDD Device\s+\d+:|\Z)"
    )
    for m in pat.finditer(output):
        path = m.group("path").strip()
        body = m.group("body")
        dev = by_key.get(path)
        if not dev:
            continue

        hm = re.search(r"^Health\s*:\s*(.+?)\s*%\s*$", body, re.M)
        if hm:
            raw = hm.group(1).strip()
            vm = re.search(r"(\d+(?:\.\d+)?)", raw)
            dev.health = vm.group(1) if vm else raw
            continue

        hm2 = re.search(r"(?im)^.*\bHealth\b.*:\s*.*?(\d+(?:\.\d+)?)\s*%", body)
        if hm2:
            dev.health = hm2.group(1)
            continue

        unk = re.search(r"(?im)^.*\bHealth\b.*:\s*\?\s*\(Unknown\)", body)
        dev.health = "Unknown" if unk else "?"

    # Dump mode uses "Hard Disk Device ... : PATH" inside per-disk sections.
    block_pat = re.compile(
        r"(?ms)^  -- Physical Disk Information - Disk:.*?\n(?P<body>.*?)(?=^  -- Physical Disk Information - Disk:|^  -- Partition Information --|\Z)"
    )
    for bm in block_pat.finditer(output):
        body = bm.group("body")
        pm = re.search(r"(?m)^\s*Hard Disk Device.*:\s*(?P<path>/\S+)\s*$", body)
        if not pm:
            continue
        path = pm.group("path").strip()
        dev = by_key.get(path)
        if not dev:
            continue

        hm = re.search(r"(?im)^.*\bHealth\b.*:\s*.*?(\d+(?:\.\d+)?)\s*%", body)
        if hm:
            dev.health = hm.group(1)
            continue

        unk = re.search(r"(?im)^.*\bHealth\b.*:\s*\?\s*\(Unknown\)", body)
        dev.health = "Unknown" if unk else dev.health

def _rewrite_hdsentinel_with_slots(output: str, devices: list[Device]) -> str:
    """Relabel HDSentinel device lines to include physical slot numbers."""
    def repl_dev(m: re.Match[str]) -> str:
        prefix, path = m.group(1), m.group(2)
        slot = _slot_for_path(path, devices)
        return f"{prefix}slot {slot}: {path}"

    out = re.sub(r"(?m)^(HDD Device\s+)\d+:\s+(\S+)", repl_dev, output)
    out = re.sub(r"(^|\r)(Device\s+)\d+(\s*:\s+)(\S+)",
                 lambda m: f"{m.group(1)}{m.group(2)}slot {_slot_for_path(m.group(4), devices)}{m.group(3)}{m.group(4)}",
                 out)
    out = re.sub(
        r"(?m)^(\s*Hard Disk Device.*:\s*)(/\S+)\s*$",
        lambda m: f"{m.group(1)}{m.group(2)}  (Slot {_slot_for_path(m.group(2), devices)})",
        out,
    )
    return out

def pick_devices(devices: list[Device], prompt: str) -> list[Device]:
    raw = prompt_input(prompt).strip()
    if raw.lower() == "all":
        return list(devices)
    selected = []
    for tok in raw.split():
        try:
            idx = int(tok)
            if 1 <= idx <= len(devices):
                selected.append(devices[idx - 1])
            else:
                print(f"  {ylw(f'Invalid: {tok} — skipping')}")
        except ValueError:
            print(f"  {ylw(f'Invalid: {tok} — skipping')}")
    return selected

def cmd_health(args: argparse.Namespace) -> bool:
    if not Path(HDSENTINEL).exists():
        sys.exit(bred(f"HDSentinel not found at {HDSENTINEL}"))
    devices = discover()
    if not devices:
        print(bred("No disks installed on specified ports."))
        return True

    enrich_lsblk(devices)
    enrich_fmt_status(devices)
    enrich_zeroed_status(devices)

    # Use stable PHY by-path symlinks for HDSentinel device targeting.
    devlist = ",".join(d.port for d in devices)
    # Always use dump internally for stable machine parsing of health fields.
    flags   = ["-dump"]
    cmd = [HDSENTINEL, "-onlydevs", devlist] + flags

    if args.raw:
        print(f"  {cyn('HDSentinel scan (raw) ...')}", flush=True)
        run = subprocess.run(cmd)
        if run.returncode != 0:
            print(ylw(f"HDSentinel exited with code {run.returncode}"))
        return True

    rc, out, err = _run_with_progress(cmd, "HDSentinel scan ...")
    _parse_hdsentinel_health(out, devices)
    for d in devices:
        log(f"HEALTH slot={d.slot} dev={d.path} model={d.model} serial={d.serial} health={d.health}%")
    print_table(devices, show_health=True)

    if args.dump:
        rewritten = _rewrite_hdsentinel_with_slots(out, devices)
        if rewritten.strip():
            print(rewritten, end="" if rewritten.endswith("\n") else "\n")
    if err.strip():
        print(err, end="" if err.endswith("\n") else "\n")

    if rc != 0:
        print(ylw(f"HDSentinel exited with code {rc}"))
    return True

def cmd_format(args: argparse.Namespace) -> bool:
    devices = discover()
    if not devices:
        print(bred("No disks installed on specified ports."))
        return True

    enrich_lsblk(devices)
    enrich_fmt_status(devices)
    enrich_zeroed_status(devices)
    print_table(devices, show_health=False)

    selected = pick_devices(devices, "Enter device numbers to format (space-separated, 'all', or Enter to cancel): ")
    if not selected:
        return False

    print(f"\n  {bold('Selected for formatting:')}")
    for d in selected:
        print(f"    {cyn(d.path)}  {dim(d.model or 'unknown')}  s/n: {d.serial or 'N/A'}")

    print()
    print(f"  {bred('!! WARNING:')} This will {bold('PERMANENTLY ERASE')} all data on the selected devices!")
    print(f"  {dim('Mode: FULL format; sg_format uses 512-byte sectors.')}")
    print()

    if prompt_input("  Type YES to confirm: ").strip() != "YES":
        log(f"FORMAT_ABORTED — devices: {' '.join(d.path for d in selected)}")
        print(ylw("\n  Aborted."))
        return False

    started: list[Device] = []
    launches: list[tuple[Device, subprocess.Popen[str]]] = []
    print()
    for d in selected:
        prep_for_format(d.path)
        print(f"  {cyn(f'==> Starting format on {d.path} ...')}")
        sg_dev = sg_device(d.path)
        if sg_dev is None:
            print(f"    {red(f'Cannot resolve sg device for {d.path}, skipping')}")
            continue
        cmd = ["sg_format", "--format", "--size=512", sg_dev]
        p = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            text=True,
        )
        launches.append((d, p))

    time.sleep(0.25)
    for d, p in launches:
        rc = p.poll()
        if rc is None or rc == 0:
            started.append(d)
            log(f"FORMAT_STARTED mode=full slot={d.slot} dev={d.path} model={d.model} serial={d.serial}")
        else:
            log(f"FORMAT_START_FAILED mode=full slot={d.slot} dev={d.path} model={d.model} serial={d.serial}")
            print(f"    {bred('FAILED')} to start format on {d.path}")

    if not started:
        print(bred("\n  No formats were started."))
        return True

    print(f"\n  {cyn('Handing off to progress monitor ...')}")
    _progress_ui(started)
    return False

def cmd_progress(args: argparse.Namespace) -> bool:
    devices = discover()
    if not devices:
        print(bred("No disks installed on specified ports."))
        return True
    enrich_lsblk(devices)
    enrich_fmt_status(devices)
    enrich_zeroed_status(devices)

    if args.devices:
        by_path = {d.path: d for d in devices}
        selected: list[Device] = []
        for p in args.devices:
            if p in by_path:
                selected.append(by_path[p])
            else:
                d = Device(p, "")
                r = subprocess.run(["lsblk", "-dn", "-o", "MODEL,SERIAL", p],
                                   capture_output=True, text=True)
                parts = r.stdout.strip().split(maxsplit=1)
                d.model = parts[0] if parts else ""
                d.serial = parts[1] if len(parts) > 1 else ""
                selected.append(d)
    else:
        print_table(devices, show_health=False)
        selected = pick_devices(devices, "Enter device numbers to monitor (space-separated, 'all', or Enter to cancel): ")
        if not selected:
            return False

    _progress_ui(selected)
    return False

_ANSI_ESC = re.compile(r'\033\[[0-9;]*[A-Za-z]')

def _vis_trunc(s: str, width: int) -> str:
    visible = 0
    i = 0
    result = []
    while i < len(s):
        m = _ANSI_ESC.match(s, i)
        if m:
            result.append(m.group())
            i = m.end()
            continue
        if visible >= width:
            break
        result.append(s[i])
        visible += 1
        i += 1
    if '\033' in ''.join(result):
        result.append('\033[0m')
    return ''.join(result)

class DevState:
    def __init__(self, device: Device):
        self.path          = device.path
        self.sg_path       = sg_device(device.path)
        self.slot          = device.slot
        self.model         = device.model
        self.serial        = device.serial
        self.status           = "waiting"
        self.ever_started     = False
        self.progress         = 0.0
        self.eta              = "--"
        self.start            = time.monotonic()
        self.format_start_time: float | None = None
        self.format_start_pct: float = 0.0
        self.fmt_status       = ""
        self.zeroed           = ""

    @property
    def elapsed(self) -> float:
        return time.monotonic() - self.start

def _poll(state: DevState) -> None:
    if state.status in ("done", "done_nostart", "failed", "lost"):
        return

    # Detect device disappearance.
    if not Path(state.path).exists():
        state.status = "lost"
        log(f"FORMAT_LOST slot={state.slot} dev={state.path} model={state.model} serial={state.serial}")
        return

    # Detect format startup timeout: still waiting after 30s with no progress ever seen.
    # Some enclosures stop reporting progress even when format has completed.
    # In that case, trust the on-disk format/zero state instead of forcing FAILED.
    if state.status == "waiting" and not state.ever_started and state.elapsed > 30:
        dev = Device(state.path, "")
        enrich_fmt_status([dev])
        enrich_zeroed_status([dev])
        state.fmt_status = dev.fmt_status
        state.zeroed = dev.zeroed
        if state.fmt_status == "512" and state.zeroed != "data":
            state.status = "done_nostart"
            state.ever_started = True
            state.progress = 100.0
            state.eta = "done"
            log(f"FORMAT_COMPLETE_FMT_TIMEOUT slot={state.slot} dev={state.path} model={state.model} serial={state.serial}")
        else:
            state.status = "failed"
            log(f"FORMAT_START_FAILED slot={state.slot} dev={state.path} model={state.model} serial={state.serial}"
                f" fmt={state.fmt_status or '?'} zeroed={state.zeroed or '?'}")
        return

    poll_dev = state.sg_path
    if poll_dev is None:
        state.status = "failed"
        log(f"FORMAT_NO_SG slot={state.slot} dev={state.path}")
        return

    outputs: list[str] = []
    for cmd in (
        ["sg_requests", poll_dev],
        ["sg_requests", "--progress", poll_dev],
    ):
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            outputs.append((r.stdout or "") + (r.stderr or ""))
        except (subprocess.TimeoutExpired, OSError, FileNotFoundError):
            continue

    if not outputs:
        return

    output = "\n".join(outputs)

    # Different HBAs/firmware return different "in progress" phrases.
    if re.search(r"progress indication|format(?:\s+command)?\s+in\s+progress|in progress.*format|not ready.*format", output, re.I):
        pct: float | None = None
        m_pct = re.search(r"(\d+(?:\.\d+)?)\s*%", output)
        if m_pct:
            pct = float(m_pct.group(1))
        else:
            m_ratio = re.search(r"\b(\d{1,5})\s*/\s*(65535|65536)\b", output)
            if m_ratio:
                pct = (float(m_ratio.group(1)) / float(m_ratio.group(2))) * 100.0

        state.ever_started = True
        state.status       = "formatting"
        if pct is not None:
            now = time.monotonic()
            if state.format_start_time is None:
                state.format_start_time = now
                state.format_start_pct  = pct
            elapsed   = now - state.format_start_time
            pct_done  = pct - state.format_start_pct
            if elapsed > 0 and pct_done > 0:
                rate      = pct_done / elapsed  # %/s
                state.eta = fmt_duration((100.0 - pct) / rate)
            else:
                state.eta = "--"
            state.progress = max(0.0, min(100.0, pct))
    elif re.search(r"error|fail|corrupted|medium error|hardware error|aborted command", output, re.I):
        state.status   = "failed"
        state.progress = 0.0
        state.eta      = "--"
        log(f"FORMAT_FAILED slot={state.slot} dev={state.path} model={state.model} serial={state.serial}")
    elif state.ever_started:
        state.status   = "done"
        state.progress = 100.0
        state.eta      = "done"
        log(f"FORMAT_COMPLETE slot={state.slot} dev={state.path} model={state.model} serial={state.serial}")

def _progress_ui(devices: list[Device]) -> None:
    states    = [DevState(d) for d in devices]
    interval  = 5
    paused    = False
    last_poll = 0.0
    first     = True

    def draw(footer: str = "", footer_green: bool = True) -> None:
        nonlocal first
        cols = shutil.get_terminal_size(fallback=(80, 24)).columns
        out  = []
        out.append("\033[2J\033[H" if first else "\033[H")
        first = False

        def _L(content: str) -> str:
            return "\r" + _vis_trunc(content, cols - 1) + "\033[K\n"

        def _divider() -> str:
            return _L(cyn('─' * (cols - 1)))

        ts = time.strftime("%Y-%m-%d  %H:%M:%S")
        out.append(_L(f"{bcyn('sg_format Progress Monitor')}   {wht(ts)}"))
        out.append(_divider())

        # Fixed column widths (visible chars). Total fixed overhead = 105 + bar_w:
        # 2 + (12+1) + (7+1) + (22+1) + (10+1) + (bar_w+2+1) + (7+1) + (20+1) + 16
        C_PATH, C_SLOT, C_SER, C_LABEL, C_PCT, C_EL, C_ETA = 12, 7, 22, 10, 7, 20, 16
        bar_w = max(4, min(40, cols - 105))

        hdr = (f"  {'Device':<{C_PATH}} {'Slot':<{C_SLOT}} {'Serial':<{C_SER}}"
               f" {'Status':<{C_LABEL}} {'Progress':^{bar_w + 2}} {'%':>{C_PCT}}"
               f" {'Elapsed':<{C_EL}} {'ETA':<{C_ETA}}")
        out.append(_L(dim(hdr)))
        sub = (f"  {'─' * C_PATH} {'─' * C_SLOT} {'─' * C_SER} {'─' * C_LABEL}"
               f" {'─' * (bar_w + 2)} {'─' * C_PCT} {'─' * C_EL} {'─' * C_ETA}")
        out.append(_L(cyn(sub)))

        for s in states:
            elapsed_str = fmt_duration(s.elapsed)

            if s.status == "done":
                col_fn, label, pct_str = bgrn, "done",       "100.00%"
            elif s.status == "done_nostart":
                col_fn, label, pct_str = bgrn, "done/nstrt", "100.00%"
            elif s.status == "failed":
                col_fn, label, pct_str = bred, "FAILED",     "  0.00%"
            elif s.status == "lost":
                col_fn, label, pct_str = bred, "LOST",       "       "
            elif s.status == "waiting":
                col_fn, label, pct_str = wht,  "waiting",    "       "
            else:
                col_fn  = bgrn if s.progress >= 50 else bylw
                label   = "formatting"
                pct_str = f"{s.progress:6.2f}%"

            eta_str = f"ETA: {s.eta}" if s.status == "formatting" and s.eta != "--" else ""

            path_f  = s.path[:C_PATH].ljust(C_PATH)
            slot_f  = f"Slot {s.slot}"[:C_SLOT].ljust(C_SLOT)
            ser_f   = f"SN:{s.serial or 'N/A'}"[:C_SER].ljust(C_SER)
            label_f = label[:C_LABEL].ljust(C_LABEL)
            pct_f   = pct_str.rjust(C_PCT)
            el_f    = f"Elapsed: {elapsed_str}"[:C_EL].ljust(C_EL)
            eta_f   = eta_str[:C_ETA].ljust(C_ETA)

            filled  = int(s.progress * bar_w / 100)
            bar_str = "#" * filled + "." * (bar_w - filled)

            body = (f"  {path_f} {slot_f} {ser_f} {label_f}"
                    f" [{bar_str}] {pct_f} {el_f} {eta_f}")
            out.append(_L(col_fn(body)))

        out.append(_divider())

        n_done = sum(1 for s in states if s.status == "done")
        n_done_nostart = sum(1 for s in states if s.status == "done_nostart")
        n_fail = sum(1 for s in states if s.status == "failed")
        n_lost = sum(1 for s in states if s.status == "lost")
        n_fmt  = sum(1 for s in states if s.status == "formatting")
        n_wait = sum(1 for s in states if s.status == "waiting")
        parts: list[str] = []
        if n_done: parts.append(bgrn(f"{n_done} done"))
        if n_done_nostart: parts.append(bgrn(f"{n_done_nostart} done/not started"))
        if n_fmt:  parts.append(grn(f"{n_fmt} formatting"))
        if n_wait: parts.append(wht(f"{n_wait} waiting"))
        if n_fail: parts.append(red(f"{n_fail} failed"))
        if n_lost: parts.append(bred(f"{n_lost} lost"))
        out.append(_L(f"  {'  |  '.join(parts) or '...'}"))

        next_in = max(0, interval - int(time.monotonic() - last_poll))
        if paused:
            out.append(_L(f"  {ylw('PAUSED')}   Interval: {interval}s"))
        else:
            out.append(_L(f"  Interval: {interval}s   Next poll in {next_in}s"))

        if footer:
            out.append(_L(""))
            out.append(_L(bgrn(footer) if footer_green else bred(footer)))

        out.append(_L(f"  {wht('[r]')} refresh   {wht('[p]')} pause/resume   {wht('[+/-]')} interval   {wht('[q]')} quit   Ctrl+C exit"))
        out.append("\033[J")

        sys.stdout.write("".join(out))
        sys.stdout.flush()

    fd  = sys.stdin.fileno()
    old = termios.tcgetattr(fd)
    sys.stdout.write("\033[?25l")
    sys.stdout.flush()

    original_sigterm = signal.getsignal(signal.SIGTERM)

    def _sigterm_handler(signum: int, frame: object) -> None:
        termios.tcsetattr(fd, termios.TCSADRAIN, old)
        sys.stdout.write("\033[?25h\n")
        sys.stdout.flush()
        sys.exit(1)

    try:
        signal.signal(signal.SIGTERM, _sigterm_handler)
        tty.setraw(fd)
        draw()
        while True:
            if select.select([sys.stdin], [], [], 0)[0]:
                ch = os.read(fd, 4)
                k  = ch[0:1]
                if   k in (b'q', b'Q', b'\x03', b'\x1b'):  break
                elif k in (b'p', b'P'):                      paused = not paused
                elif k in (b'r', b'R'):                      last_poll = 0.0
                elif k == b'+':                              interval = min(60, interval + 1)
                elif k == b'-':                              interval = max(1,  interval - 1)

            now = time.monotonic()
            if not paused and (now - last_poll) >= interval:
                last_poll = now
                for s in states:
                    _poll(s)

            if all(s.status in ("done", "done_nostart", "failed", "lost") for s in states) and any(s.ever_started for s in states):
                n_f = sum(1 for s in states if s.status in ("failed", "lost"))
                if n_f:
                    draw(f"  Finished — {n_f} disk(s) FAILED.  Press q to exit.", footer_green=False)
                else:
                    draw("  All formatting complete.  Press q to exit.")
                while True:
                    if select.select([sys.stdin], [], [], 0.2)[0]:
                        ch = os.read(fd, 4)
                        if ch[0:1] in (b'q', b'Q', b'\x1b', b'\x03'):
                            break
                break

            draw()
            time.sleep(0.2)
    finally:
        signal.signal(signal.SIGTERM, original_sigterm)
        termios.tcsetattr(fd, termios.TCSADRAIN, old)
        sys.stdout.write("\033[?25h\n")
        sys.stdout.flush()

def cmd_speedtest(args: argparse.Namespace) -> bool:
    devices = discover()
    if not devices:
        print(bred("No disks installed on specified ports."))
        return True

    enrich_lsblk(devices)
    print_table(devices, show_health=False)

    selected = pick_devices(devices, "Enter device numbers to test (space-separated, 'all', or Enter to cancel): ")
    if not selected:
        return False

    print()

    def _disk_size(path: str) -> int:
        r = subprocess.run(["blockdev", "--getsize64", path], capture_output=True, text=True, timeout=30)
        try:
            return int(r.stdout.strip())
        except ValueError:
            return 0

    def _run_read(path: str) -> str:
        try:
            r = subprocess.run(
                ["dd", f"if={path}", "of=/dev/null", "bs=4M", "count=256", "iflag=direct"],
                capture_output=True, text=True, timeout=30,
            )
        except subprocess.TimeoutExpired:
            return "timeout"
        m_bytes = re.search(r"^(\d+) bytes", r.stderr, re.M)
        if m_bytes and int(m_bytes.group(1)) == 0:
            return "no data"
        m = re.search(r"([\d.]+)\s*(GB/s|MB/s|kB/s)", r.stderr)
        return f"{m.group(1)} {m.group(2)}" if m else "?"

    def _spd_col(spd: str) -> str:
        if spd in ("?", "timeout", "no data"):
            return bred(spd)
        m = re.match(r"([\d.]+)", spd)
        if not m:
            return ylw(spd)
        return bgrn(spd) if float(m.group(1)) >= 100 else ylw(spd)

    results: list[tuple[Device, str]] = []
    for i, d in enumerate(selected, 1):
        print(f"  {cyn(f'[{i}/{len(selected)}]')}  {d.path}  Slot {d.slot}")

        if _disk_size(d.path) == 0:
            print(f"    {bred('disk reports 0 bytes — rescan needed (blockdev --rereadpt ' + d.path + ')')}")
            results.append((d, "no data"))
            continue

        print(f"    read (1 GiB) ...", end=" ", flush=True)
        spd = _run_read(d.path)
        print(_spd_col(spd))
        results.append((d, spd))

    if len(results) > 1:
        print(f"\n  {bold('Summary:')}")
        print(bold(f"    {'Slot':<6}  {'Device':<12}  {'Read':>12}"))
        for d, spd in results:
            spd_str = _spd_col(spd)
            spd_pad = " " * max(0, 12 - _visible_len(spd_str))
            print(f"    {str(d.slot):<6}  {d.path:<12}  {spd_pad}{spd_str}")

    log(
        "SPEEDTEST " + " ".join(
            f"serial={d.serial or '?'} dev={d.path} slot={d.slot} read={spd}"
            for d, spd in results
        )
    )
    return True

def cmd_debug_zero(args: argparse.Namespace) -> bool:
    """Debug zero detection on a specific device."""
    import sys
    if not args.device:
        print(bred("Usage: python disk.py debug-zero /dev/sdX"))
        return False

    dev_path = args.device
    print(f"\n  {bold('Debugging zero detection for:')} {cyn(dev_path)}\n")

    # Get size
    r = subprocess.run(["blockdev", "--getsize64", dev_path],
                      capture_output=True, text=True, timeout=30)
    total_bytes = int(r.stdout.strip())
    print(f"  Total size: {total_bytes / (1024**4):.2f} TiB ({total_bytes} bytes)")

    sample_size = 4096
    sample_count = 5
    positions = _zero_sample_positions(total_bytes, sample_size=sample_size, sample_count=sample_count)
    if not positions:
        print(f"  {ylw('Could not build sample positions.')}")
        return False

    for idx, pos in enumerate(positions):
        pct = 0 if len(positions) == 1 else round((idx * 100) / (len(positions) - 1))
        print(f"\n  Sample {idx + 1} ({pct}%) (byte {pos}):")
        try:
            r = subprocess.run(
                ["dd", f"if={dev_path}", "of=/dev/stdout", f"skip={pos // 512}",
                 "bs=512", "count=8", "iflag=direct"],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10
            )
            if r.returncode != 0:
                print(f"    {ylw('dd failed:')} {r.stderr.decode('utf-8', errors='ignore').strip()}")
                continue

            if not r.stdout:
                print(f"    {ylw('No output from dd')}")
                continue

            zero_count = sum(1 for b in r.stdout if b == 0)
            non_zero_count = len(r.stdout) - zero_count
            has_data = any(b != 0 for b in r.stdout)

            print(f"    Read {len(r.stdout)} bytes")
            print(f"    Zero bytes: {zero_count}, Non-zero bytes: {non_zero_count}")
            print(f"    Status: {bgrn('ZERO') if not has_data else bred('DATA')}")

            if has_data:
                # Show first few non-zero bytes
                sample = r.stdout[:64]
                hex_str = " ".join(f"{b:02x}" for b in sample)
                print(f"    First 64 bytes (hex): {hex_str}")
        except Exception as e:
            print(f"    {bred(f'Error: {e}')}")

    return True

def cmd_missing(args: argparse.Namespace) -> bool:
    devices = discover()
    enrich_lsblk(devices)
    enrich_fmt_status(devices)
    enrich_zeroed_status(devices)
    by_port = {d.port: d for d in devices}
    ordered_ports = sorted(PORTS, key=lambda p: PORT_TO_SLOT.get(p, 999))

    slot_w, st_w, fmt_w, dev_w, ser_w, mdl_w = 4, 8, 5, 12, 22, 20
    print()
    print(f"  {bold('Expected SLOT status:')}")
    print("  " + _pad(bold("Slot"), slot_w) + "  " +
          _pad(bold("Status"), st_w) + "   " + _pad(bold("Fmt"), fmt_w) + "  " +
          _pad(bold("Device"), dev_w) + "  " +
          _pad(bold("Serial"), ser_w) + "  " + _pad(bold("Model"), mdl_w))
    print(f"  {'─'*slot_w}  {'─'*st_w}   {'─'*fmt_w}  {'─'*dev_w}  {'─'*ser_w}  {'─'*mdl_w}")

    missing: list[tuple[int | str, str]] = []
    for port in ordered_ports:
        slot = PORT_TO_SLOT.get(port, "?")
        dev = by_port.get(port)
        if dev:
            fmt_fld = _fmt_status_color(dev.fmt_status, dev.zeroed)
            print("  " + _pad(str(slot), slot_w) + "  " +
                  _pad(bgrn("PRESENT"), st_w) + "   " + _pad(fmt_fld, fmt_w) + "  " +
                  _pad(dev.path, dev_w) + "  " +
                  _pad(dev.serial or "N/A", ser_w) + "  " + _pad(dev.model or "unknown", mdl_w))
        else:
            print("  " + _pad(str(slot), slot_w) + "  " +
                  _pad(bred("MISSING"), st_w) + "   " + _pad("-", fmt_w) + "  " +
                  _pad("-", dev_w) + "  " +
                  _pad("-", ser_w) + "  " + _pad("-", mdl_w))
            missing.append((slot, port))

    print()
    if missing:
        missing_slots = ", ".join(str(slot) for slot, _ in sorted(missing, key=lambda x: (x[0] if isinstance(x[0], int) else 999)))
        print(f"  {bred(f'Missing: {len(missing)} slot(s): {missing_slots}')}")
        log(f"MISSING_SLOTS count={len(missing)} slots={missing_slots}")
    else:
        print(f"  {bgrn('All expected slots are present.')}")
        log("MISSING_SLOTS count=0")
    return True

def main_menu() -> None:
    MENU = [
        ("Health check",            cmd_health,     argparse.Namespace(dump=False, raw=False)),
        ("Format disks",            cmd_format,     argparse.Namespace()),
        ("Monitor format progress", cmd_progress,   argparse.Namespace(devices=[])),
        ("Speed test",              cmd_speedtest,  argparse.Namespace()),
        ("Show missing slots",      cmd_missing,    argparse.Namespace()),
    ]

    while True:
        if _TTY:
            print("\033[2J\033[H", end="", flush=True)

        print()
        print(f"  {bcyn('Disk Management Tool')}")
        print(f"  {'─' * 28}")
        print()
        for i, (label, _, _) in enumerate(MENU, 1):
            print(f"  {ylw(bold(f'[{i}]'))}  {label}")
        print()
        print(f"  {dim('[q]')}  Quit")
        print()
        print(f"  {dim('Ctrl+C at any time to exit immediately')}")
        print()

        choice = input(f"  {bold('Choose:')} ").strip().lower()
        if choice == "q":
            break
        try:
            idx = int(choice)
            if 1 <= idx <= len(MENU):
                _, fn, ns = MENU[idx - 1]
                print()
                needs_pause = fn(ns)
                if needs_pause:
                    input(f"\n  {bold('Press Enter to return to menu...')} ")
                continue
        except ValueError:
            pass
        print(f"  {ylw('Invalid choice.')}")

def main() -> None:
    require_root()

    parser = argparse.ArgumentParser(
        prog="disk",
        description="Disk health, format, and progress monitoring.",
    )
    sub = parser.add_subparsers(dest="cmd")

    p_health = sub.add_parser("health",   help="Show disk health via HDSentinel")
    p_health.add_argument("--dump", action="store_true", help="Full detailed report")
    p_health.add_argument("--raw", action="store_true", help="Show raw HDSentinel output (original behavior)")

    sub.add_parser("format",   help="Interactive low-level format (512-byte sectors)")

    p_prog = sub.add_parser("progress", help="Monitor sg_format progress")
    p_prog.add_argument("devices", nargs="*", metavar="DEV",
                        help="Devices to monitor (omit for interactive selection)")

    sub.add_parser("speedtest", help="Read/write speed test (1 GiB per disk)")
    sub.add_parser("missing", help="Show expected PHY slots and highlight missing disks")

    p_debug = sub.add_parser("debug-zero", help="Debug zero detection on a specific device")
    p_debug.add_argument("device", help="Device path (e.g., /dev/sda)")

    args = parser.parse_args()

    if not args.cmd:
        main_menu()
        return

    dispatch = {
        "health": cmd_health,
        "format": cmd_format,
        "progress": cmd_progress,
        "speedtest": cmd_speedtest,
        "missing": cmd_missing,
        "debug-zero": cmd_debug_zero,
    }
    dispatch[args.cmd](args)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print()
