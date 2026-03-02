#!/usr/bin/env python3
"""disk.py — disk health, format, and progress monitoring tool."""

from __future__ import annotations

import argparse
import logging
import os
import pty
import re
import select
import shutil
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

SLOTS = [2, 5, 8, 11, 1, 4, 7, 10, 0, 3, 6, 9]
PORT_TO_SLOT = dict(zip(PORTS, SLOTS))

HDSENTINEL = "/root/HDSentinel"
LOGFILE    = "/var/log/diskops.log"

_log = logging.getLogger("diskops")
_log.setLevel(logging.INFO)
try:
    _log.addHandler(logging.FileHandler(LOGFILE))
except PermissionError:
    pass

def log(msg: str) -> None:
    _log.info(msg)

_TTY = sys.stdout.isatty()
GRN = "\033[32m"; BGRN = "\033[1;32m"; YLW = "\033[33m"; BYLW = "\033[1;33m"; RED = "\033[31m"
CYN = "\033[36m"; BCYN = "\033[1;36m"; WHT = "\033[37m"; RST = "\033[0m"

def _a(code: str, t: str) -> str:
    return f"\033[{code}m{t}\033[0m" if _TTY else t

def bold(t: str) -> str: return _a("1", t)
def dim(t: str) -> str:  return _a("2", t)
def ylw(t: str) -> str:  return _a("33", t)
def cyn(t: str) -> str:  return _a("36", t)
def bred(t: str) -> str: return _a("1;31", t)
def bgrn(t: str) -> str: return _a("1;32", t)
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
                sys.stdout.write("\n")
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
        Path(f"{sysfs}/timeout").write_text("5")
    except OSError:
        pass

def sg_device(dev_path: str) -> str:
    name = Path(dev_path).name
    sg_dir = Path(f"/sys/block/{name}/device/scsi_generic")
    try:
        sg_name = next(sg_dir.iterdir()).name
        return f"/dev/{sg_name}"
    except (StopIteration, OSError):
        return dev_path

class Device:
    def __init__(self, path: str, port: str):
        self.path   = path
        self.port   = port
        self.slot   = PORT_TO_SLOT.get(port, "?")
        self.model  = ""
        self.serial = ""
        self.health = "?"
        self.size   = ""

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
        capture_output=True, text=True,
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
                                    capture_output=True, text=True)
                m = re.search(r"Serial number:\s*(\S+)", r2.stdout, re.I)
                if m:
                    dev.serial = m.group(1)

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
    idx_w, dev_w, size_w, hlth_w, ser_w, mdl_w, slot_w = 4, 12, 8, 8, 22, 20, 4
    _n, _dev, _sz, _hlth, _ser, _mdl = "#", "Device", "Size", "Health", "Serial", "Model"
    print()
    if show_health:
        print("  " + _pad(bold(_n), idx_w) + " " + _pad(bold(_dev), dev_w) + "  " +
              _pad(bold(_sz), size_w) + "  " + _pad(bold(_hlth), hlth_w) + "  " +
              _pad(bold(_ser), ser_w) + "  " + _pad(bold(_mdl), mdl_w) + "  " + _pad(bold("Slot"), slot_w))
        print(f"  {'─'*idx_w} {'─'*dev_w}  {'─'*size_w}  {'─'*hlth_w}  {'─'*ser_w}  {'─'*mdl_w}  {'─'*slot_w}")
        for i, d in enumerate(devices, 1):
            h_fld    = _health_color(d.health) if d.health not in ("?", "--") else dim(d.health)
            slot_fld = cyn(str(d.slot))
            print("  " + _pad(f"[{i:<2}]", idx_w) + " " + _pad(d.path, dev_w) + "  " +
                  _pad(d.size, size_w) + "  " + _pad(h_fld, hlth_w) + "  " +
                  _pad(d.serial or "N/A", ser_w) + "  " + _pad(d.model or "unknown", mdl_w) + "  " +
                  _pad(slot_fld, slot_w))
    else:
        print("  " + _pad(bold(_n), idx_w) + " " + _pad(bold(_dev), dev_w) + "  " +
              _pad(bold(_sz), size_w) + "  " + _pad(bold(_ser), ser_w) + "  " +
              _pad(bold(_mdl), mdl_w) + "  " + _pad(bold("Slot"), slot_w))
        print(f"  {'─'*idx_w} {'─'*dev_w}  {'─'*size_w}  {'─'*ser_w}  {'─'*mdl_w}  {'─'*slot_w}")
        for i, d in enumerate(devices, 1):
            slot_fld = cyn(str(d.slot))
            print("  " + _pad(f"[{i:<2}]", idx_w) + " " + _pad(d.path, dev_w) + "  " +
                  _pad(d.size, size_w) + "  " + _pad(d.serial or "N/A", ser_w) + "  " +
                  _pad(d.model or "unknown", mdl_w) + "  " + _pad(slot_fld, slot_w))
    print()

def _run_with_progress(cmd: list[str], label: str) -> tuple[int, str, str]:
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
        print(ylw("No disks connected on specified ports."))
        return False

    enrich_lsblk(devices)

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
        print(ylw("No disks found on specified ports."))
        return False

    enrich_lsblk(devices)
    print_table(devices, show_health=False)

    selected = pick_devices(devices, "Enter device numbers to format (space-separated, 'all', or Enter to cancel): ")
    if not selected:
        return False

    fast_mode = bool(getattr(args, "fast", False))
    if not fast_mode:
        mode_in = input(bold("Format mode: [n]ormal (default) or [f]ast: ")).strip().lower()
        if mode_in in ("f", "fast"):
            fast_mode = True

    print(f"\n  {bold('Selected for formatting:')}")
    for d in selected:
        print(f"    {cyn(d.path)}  {dim(d.model or 'unknown')}  s/n: {d.serial or 'N/A'}")

    print()
    print(f"  {bred('!! WARNING:')} This will {bold('PERMANENTLY ERASE')} all data on the selected devices!")
    mode_label = "FAST format (ffmt=1)" if fast_mode else "FULL format"
    print(f"  {dim(f'Mode: {mode_label}; sg_format uses 512-byte sectors.')}")
    print()

    if input(bold("  Type YES to confirm: ")).strip() != "YES":
        log(f"FORMAT_ABORTED — devices: {' '.join(d.path for d in selected)}")
        print(ylw("\n  Aborted."))
        return False

    started: list[Device] = []
    launches: list[tuple[Device, subprocess.Popen[str]]] = []
    print()
    for d in selected:
        prep_for_format(d.path)
        print(f"  {cyn(f'==> Starting format on {d.path} ...')}")
        cmd = ["sg_format", "--format", "--size=512"]
        if fast_mode:
            cmd.append("--ffmt=1")
        cmd.append(sg_device(d.path))
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
            mode = "fast" if fast_mode else "full"
            log(f"FORMAT_STARTED mode={mode} slot={d.slot} dev={d.path} model={d.model} serial={d.serial}")
        else:
            mode = "fast" if fast_mode else "full"
            log(f"FORMAT_START_FAILED mode={mode} slot={d.slot} dev={d.path} model={d.model} serial={d.serial}")
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
        print(ylw("No disks found on specified ports."))
        return False
    enrich_lsblk(devices)

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
        self.status        = "waiting"
        self.ever_started  = False
        self.progress      = 0.0
        self.eta           = "--"
        self.start         = time.monotonic()
        self.prev_pct      = 0.0
        self.prev_time     = time.monotonic()

    @property
    def elapsed(self) -> float:
        return time.monotonic() - self.start

def _poll(state: DevState) -> None:
    if state.status in ("done", "failed"):
        return
    poll_dev = state.sg_path
    outputs: list[str] = []
    for cmd in (
        ["sg_requests", poll_dev],
        ["sg_requests", "--progress", poll_dev],
    ):
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
            outputs.append((r.stdout or "") + (r.stderr or ""))
        except subprocess.TimeoutExpired:
            continue

    if not outputs:
        return

    output = "\n".join(outputs)

    if re.search(r"progress indication|format in progress", output, re.I):
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
            now  = time.monotonic()
            dt   = now - state.prev_time
            dp   = pct - state.prev_pct
            state.eta       = fmt_duration((100.0 - pct) / (dp / dt)) if dt > 0 and dp > 0 else "--"
            state.prev_pct  = pct
            state.prev_time = now
            state.progress  = max(0.0, min(100.0, pct))
    elif re.search(r"error|fail|corrupted|medium error|hardware error|aborted command|not ready", output, re.I):
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
            return _L(f"{CYN}{'─' * (cols - 1)}{RST}")

        ts = time.strftime("%Y-%m-%d  %H:%M:%S")
        out.append(_L(f"{BCYN}sg_format Progress Monitor{RST}   {WHT}{ts}{RST}"))
        out.append(_divider())

        for s in states:
            elapsed_str = fmt_duration(s.elapsed)

            if s.status == "done":
                col      = BGRN
                label    = "done"
                pct_str  = "100.00%"
            elif s.status == "failed":
                col      = RED
                label    = "FAILED"
                pct_str  = "  0.00%"
            elif s.status == "waiting":
                col      = WHT
                label    = "waiting"
                pct_str  = ""
            else:
                col      = BGRN if s.progress >= 50 else BYLW
                label    = "formatting"
                pct_str  = f"{s.progress:6.2f}%"
            slot_str    = f"Slot {s.slot}"
            serial_str  = f"SN:{s.serial or 'N/A'}"
            left_prefix = f"  {s.path}  {slot_str}  {serial_str}  {label}  "
            right_info  = f"  Elapsed: {elapsed_str}"
            if s.status == "formatting" and s.eta != "--":
                right_info += f"  ETA: {s.eta}"

            if s.status == "formatting":
                fixed_visible = len(left_prefix) + 2 + 1 + len(pct_str) + len(right_info)
                bar_width     = max(8, min(48, cols - fixed_visible - 2))
                filled        = int(s.progress * bar_width / 100)
                bar           = "#" * filled + "." * (bar_width - filled)
                body          = f"{left_prefix}[{bar}] {pct_str}{right_info}"
            else:
                body = f"{left_prefix}{right_info.lstrip()}"

            out.append(_L(f"{col}{body}{RST}"))

        out.append(_divider())

        n_done = sum(1 for s in states if s.status == "done")
        n_fail = sum(1 for s in states if s.status == "failed")
        n_fmt  = sum(1 for s in states if s.status == "formatting")
        n_wait = sum(1 for s in states if s.status == "waiting")
        parts: list[str] = []
        if n_done: parts.append(f"{BGRN}{n_done} done{RST}")
        if n_fmt:  parts.append(f"{GRN}{n_fmt} formatting{RST}")
        if n_wait: parts.append(f"{WHT}{n_wait} waiting{RST}")
        if n_fail: parts.append(f"{RED}{n_fail} failed{RST}")
        out.append(_L(f"  {'  |  '.join(parts) or '...'}"))

        next_in = max(0, interval - int(time.monotonic() - last_poll))
        if paused:
            out.append(_L(f"  {YLW}PAUSED{RST}   Interval: {interval}s"))
        else:
            out.append(_L(f"  Interval: {interval}s   Next poll in {next_in}s"))

        if footer:
            fc = BGRN if footer_green else RED
            out.append(_L(""))
            out.append(_L(f"{fc}{footer}{RST}"))

        out.append(_L(f"  {WHT}[r]{RST} refresh   {WHT}[p]{RST} pause/resume   {WHT}[+/-]{RST} interval   {WHT}[q]{RST} quit   Ctrl+C exit"))
        out.append("\033[J")

        sys.stdout.write("".join(out))
        sys.stdout.flush()

    fd  = sys.stdin.fileno()
    old = termios.tcgetattr(fd)
    sys.stdout.write("\033[?25l")
    sys.stdout.flush()
    try:
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

            if all(s.status in ("done", "failed") for s in states) and any(s.ever_started for s in states):
                n_f = sum(1 for s in states if s.status == "failed")
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
        termios.tcsetattr(fd, termios.TCSADRAIN, old)
        sys.stdout.write("\033[?25h\n")
        sys.stdout.flush()

def cmd_speedtest(args: argparse.Namespace) -> bool:
    devices = discover()
    if not devices:
        print(ylw("No disks found on specified ports."))
        return False

    enrich_lsblk(devices)
    print_table(devices, show_health=False)

    selected = pick_devices(devices, "Enter device numbers to test (space-separated, 'all', or Enter to cancel): ")
    if not selected:
        return False

    print()

    def _disk_size(path: str) -> int:
        r = subprocess.run(["blockdev", "--getsize64", path], capture_output=True, text=True)
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
    for d in selected:
        print(f"  {cyn(f'{d.path}  Slot {d.slot}')}")

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
            print(f"    {str(d.slot):<6}  {d.path:<12}  {_spd_col(spd):>12}")

    log("SPEEDTEST " + " ".join(f"slot={d.slot} read={spd}" for d, spd in results))
    return True

def cmd_missing(args: argparse.Namespace) -> bool:
    devices = discover()
    enrich_lsblk(devices)
    by_port = {d.port: d for d in devices}
    ordered_ports = sorted(PORTS, key=lambda p: PORT_TO_SLOT.get(p, 999))

    slot_w, phy_w, st_w, dev_w, ser_w, mdl_w = 4, 3, 8, 12, 22, 20
    print()
    print(f"  {bold('Expected PHY/SLOT status:')}")
    print("  " + _pad(bold("Slot"), slot_w) + "  " + _pad(bold("PHY"), phy_w) + "  " +
          _pad(bold("Status"), st_w) + "   " + _pad(bold("Device"), dev_w) + "  " +
          _pad(bold("Serial"), ser_w) + "  " + _pad(bold("Model"), mdl_w))
    print(f"  {'─'*slot_w}  {'─'*phy_w}  {'─'*st_w}   {'─'*dev_w}  {'─'*ser_w}  {'─'*mdl_w}")

    missing: list[tuple[int | str, str]] = []
    for port in ordered_ports:
        slot = PORT_TO_SLOT.get(port, "?")
        m = re.search(r"-phy(\d+)-", port)
        phy = m.group(1) if m else "?"
        dev = by_port.get(port)
        if dev:
            print("  " + _pad(str(slot), slot_w) + "  " + _pad(phy, phy_w) + "  " +
                  _pad(bgrn("PRESENT"), st_w) + "   " + _pad(dev.path, dev_w) + "  " +
                  _pad(dev.serial or "N/A", ser_w) + "  " + _pad(dev.model or "unknown", mdl_w))
        else:
            print("  " + _pad(str(slot), slot_w) + "  " + _pad(phy, phy_w) + "  " +
                  _pad(bred("MISSING"), st_w) + "   " + _pad("-", dev_w) + "  " +
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
        ("Format disks",            cmd_format,     argparse.Namespace(fast=False)),
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

    p_fmt = sub.add_parser("format",   help="Interactive low-level format (512-byte sectors)")
    p_fmt.add_argument("--fast", action="store_true",
                       help="Use fast format mode (sg_format --ffmt=1)")

    p_prog = sub.add_parser("progress", help="Monitor sg_format progress")
    p_prog.add_argument("devices", nargs="*", metavar="DEV",
                        help="Devices to monitor (omit for interactive selection)")

    sub.add_parser("speedtest", help="Read/write speed test (1 GiB per disk)")
    sub.add_parser("missing", help="Show expected PHY slots and highlight missing disks")

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
    }
    dispatch[args.cmd](args)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print()
