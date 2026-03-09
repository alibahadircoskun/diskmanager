#!/usr/bin/env python3
"""app.py — Flask web backend for disk management tool."""

from __future__ import annotations

import argparse
import datetime
import logging
import os
import re
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

# ---------------------------------------------------------------------------
# Import disk.py as a module
# ---------------------------------------------------------------------------
sys.path.insert(0, str(Path(__file__).parent.parent))
import disk as _disk

from flask import Flask, jsonify, request, send_from_directory
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
if os.geteuid() != 0:
    sys.exit("This web server must be run as root.")

app = Flask(__name__, static_folder="static")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("diskweb")

# ---------------------------------------------------------------------------
# Configuration
# NOTE: This is an internal tool used by ~5 people. Security is not a
# primary concern — prioritize functionality over strict hardening.
# ---------------------------------------------------------------------------

_RATE_DEFAULT     = os.environ.get("RATE_LIMIT_DEFAULT",     "60 per minute")
_RATE_DESTRUCTIVE = os.environ.get("RATE_LIMIT_DESTRUCTIVE", "20 per minute")

app.config["MAX_CONTENT_LENGTH"] = 64 * 1024  # 64 KB max request body

limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=[_RATE_DEFAULT],
    storage_uri="memory://",
)

@app.errorhandler(404)
def not_found(e):
    return jsonify({"error": "Not found"}), 404

@app.errorhandler(429)
def rate_limited(e):
    return jsonify({"error": "Rate limit exceeded — please wait before retrying."}), 429

@app.errorhandler(Exception)
def handle_exception(e):
    log.exception("Unhandled exception")
    return jsonify({"error": str(e)}), 500

@app.after_request
def add_security_headers(response):
    response.headers["X-Content-Type-Options"]  = "nosniff"
    response.headers["X-Frame-Options"]         = "DENY"
    response.headers["Cache-Control"]           = "no-store"
    response.headers["Content-Security-Policy"] = "default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'"
    return response

# ---------------------------------------------------------------------------
# Shared progress state for format polling
# key: device path -> DevState
# ---------------------------------------------------------------------------
_progress_states: dict[str, _disk.DevState] = {}
_progress_lock = threading.Lock()
_polling_cancelled = threading.Event()

# Registry of launched format jobs
_format_jobs: dict[str, list[str]] = {}
_format_jobs_lock = threading.Lock()

# Registry of format subprocesses for reaping
_format_procs: dict[str, subprocess.Popen] = {}
_format_procs_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def device_to_dict(d: _disk.Device) -> dict:
    return {
        "path": d.path,
        "port": d.port,
        "slot": d.slot,
        "model": d.model,
        "serial": d.serial,
        "health": d.health,
        "size": d.size,
        "fmt_status": d.fmt_status,
        "zeroed": d.zeroed,
    }


def devstate_to_dict(s: _disk.DevState) -> dict:
    return {
        "path": s.path,
        "slot": s.slot,
        "model": s.model,
        "serial": s.serial,
        "status": s.status,
        "progress": s.progress,
        "eta": s.eta,
        "elapsed": round(s.elapsed, 1),
        "ever_started": s.ever_started,
    }


def _get_devices_enriched() -> list[_disk.Device]:
    devices = _disk.discover()
    if devices:
        _disk.enrich_lsblk(devices)
        _disk.enrich_fmt_status(devices)
        _disk.enrich_zeroed_status(devices)
    return devices


def disk_logfile_path() -> str:
    return str(Path(_disk.LOGFILE).resolve())


def tail_log_lines(path: Path, limit: int = 500) -> list[str]:
    if limit <= 0:
        return []
    chunk_size = 8192
    chunks: list[bytes] = []
    newline_count = 0
    with path.open("rb") as f:
        f.seek(0, os.SEEK_END)
        pos = f.tell()
        while pos > 0 and newline_count <= limit:
            read_size = min(chunk_size, pos)
            pos -= read_size
            f.seek(pos)
            chunk = f.read(read_size)
            chunks.append(chunk)
            newline_count += chunk.count(b"\n")
    payload = b"".join(reversed(chunks))
    return payload.decode("utf-8", errors="replace").splitlines()[-limit:]


# ---------------------------------------------------------------------------
# Static files
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


@app.route("/api/logs")
def api_logs():
    source = disk_logfile_path()
    path = Path(source)
    try:
        lines = tail_log_lines(path, limit=500)
        return jsonify({
            "ok": True,
            "source": source,
            "line_count": len(lines),
            "lines": lines,
            "fetched_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        })
    except FileNotFoundError:
        return jsonify({
            "ok": False,
            "source": source,
            "error": "Log file not found.",
        }), 404
    except PermissionError:
        return jsonify({
            "ok": False,
            "source": source,
            "error": "Log file is unreadable (permission denied).",
        }), 403
    except OSError as e:
        return jsonify({
            "ok": False,
            "source": source,
            "error": f"Failed to read log file: {e}",
        }), 500


# ---------------------------------------------------------------------------
# REST: Discover & Missing
# ---------------------------------------------------------------------------

@app.route("/api/discover")
def api_discover():
    try:
        devices = _get_devices_enriched()
        return jsonify([device_to_dict(d) for d in devices])
    except Exception as e:
        log.exception("discover failed")
        return jsonify({"error": str(e)}), 500


@app.route("/api/missing")
def api_missing():
    try:
        devices = _get_devices_enriched()
        by_port = {d.port: d for d in devices}
        ordered_ports = sorted(_disk.PORTS, key=lambda p: _disk.PORT_TO_SLOT.get(p, 999))
        slots = []
        for port in ordered_ports:
            slot = _disk.PORT_TO_SLOT.get(port, "?")
            m = re.search(r"-phy(\d+)-", port)
            phy = m.group(1) if m else "?"
            dev = by_port.get(port)
            slots.append({
                "slot": slot,
                "phy": phy,
                "status": "PRESENT" if dev else "MISSING",
                "device": device_to_dict(dev) if dev else None,
            })
        return jsonify({"slots": slots})
    except Exception as e:
        log.exception("missing failed")
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# REST: Health scan (blocking — can take 30-120s)
# ---------------------------------------------------------------------------

@app.route("/api/health", methods=["POST"])
@limiter.limit(_RATE_DESTRUCTIVE)
def api_health():
    try:
        devices = _disk.discover()
        if not devices:
            return jsonify({"devices": [], "rc": 0, "elapsed": 0,
                            "message": "No disks found on specified ports."})

        _disk.enrich_lsblk(devices)
        _disk.enrich_fmt_status(devices)
        _disk.enrich_zeroed_status(devices)

        if not Path(_disk.HDSENTINEL).exists():
            return jsonify({"error": f"HDSentinel not found at {_disk.HDSENTINEL}"}), 500

        devlist = ",".join(d.port for d in devices)
        cmd = [_disk.HDSENTINEL, "-onlydevs", devlist, "-dump"]

        start = time.monotonic()
        proc = subprocess.run(cmd, capture_output=True, timeout=300)
        elapsed = time.monotonic() - start

        out = proc.stdout.decode("utf-8", errors="replace")
        _disk._parse_hdsentinel_health(out, devices)

        for d in devices:
            _disk.log(f"HEALTH slot={d.slot} dev={d.path} model={d.model} serial={d.serial} health={d.health}%")

        return jsonify({
            "devices": [device_to_dict(d) for d in devices],
            "rc": proc.returncode,
            "elapsed": round(elapsed, 1),
        })
    except subprocess.TimeoutExpired:
        return jsonify({"error": "HDSentinel timed out after 5 minutes"}), 500
    except Exception as e:
        log.exception("health failed")
        return jsonify({"error": str(e)}), 500


# ---------------------------------------------------------------------------
# REST: Format start
# ---------------------------------------------------------------------------

@app.route("/api/format/start", methods=["POST"])
@limiter.limit(_RATE_DESTRUCTIVE)
def api_format_start():
    data = request.get_json()
    if not data or "devices" not in data:
        return jsonify({"error": "Missing 'devices' field"}), 400

    requested_paths: list[str] = data["devices"]
    if not requested_paths:
        return jsonify({"error": "No devices specified"}), 400

    discovered = _disk.discover()
    _disk.enrich_lsblk(discovered)
    valid_paths = {d.path for d in discovered}
    by_path = {d.path: d for d in discovered}

    started = []
    failed = []

    # Starting a new format run re-enables polling globally.
    _polling_cancelled.clear()

    for path in requested_paths:
        if path not in valid_paths:
            failed.append({"path": path, "reason": "device not found"})
            continue
        dev = by_path[path]
        try:
            if not Path(path).is_block_device():
                failed.append({"path": path, "reason": "device disappeared before format launch"})
                continue
            _disk.prep_for_format(path)
            sg_dev = _disk.sg_device(path)
            if sg_dev is None:
                failed.append({"path": path, "reason": "cannot resolve sg device"})
                continue
            cmd = ["sg_format", "--format", "--size=512", sg_dev]
            p = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            time.sleep(0.25)
            rc = p.poll()
            if rc is None or rc == 0:
                started.append(path)
                _disk.log(f"FORMAT_STARTED mode=full slot={dev.slot} dev={path} model={dev.model} serial={dev.serial}")
                with _format_procs_lock:
                    _format_procs[path] = p
                with _progress_lock:
                    _progress_states[path] = _disk.DevState(dev)
            else:
                failed.append({"path": path, "reason": f"sg_format exited with code {rc}"})
                _disk.log(f"FORMAT_START_FAILED mode=full slot={dev.slot} dev={path}")
        except Exception as e:
            failed.append({"path": path, "reason": str(e)})

    job_id = str(uuid.uuid4())
    with _format_jobs_lock:
        _format_jobs[job_id] = started

    return jsonify({"job_id": job_id, "started": started, "failed": failed})


# ---------------------------------------------------------------------------
# REST: Monitor polling control
# ---------------------------------------------------------------------------

@app.route("/api/format/poll/cancel", methods=["POST"])
@limiter.limit(_RATE_DESTRUCTIVE)
def api_format_poll_cancel():
    _polling_cancelled.set()
    return jsonify({"ok": True, "cancelled": True})


@app.route("/api/format/poll/resume", methods=["POST"])
@limiter.limit(_RATE_DESTRUCTIVE)
def api_format_poll_resume():
    _polling_cancelled.clear()
    return jsonify({"ok": True, "cancelled": False})


# ---------------------------------------------------------------------------
# REST: Format progress poll
# ---------------------------------------------------------------------------

@app.route("/api/format/poll", methods=["POST"])
def api_format_poll():
    data = request.get_json()
    if not data or "devices" not in data:
        return jsonify({"error": "Missing 'devices' field"}), 400

    device_paths: list[str] = data["devices"]

    if _polling_cancelled.is_set():
        return jsonify({"error": "Polling cancelled by another session."}), 409

    # Allow monitor-only sessions (without /api/format/start) by lazily
    # creating progress state for currently discovered devices.
    discovered = _disk.discover()
    _disk.enrich_lsblk(discovered)
    by_path = {d.path: d for d in discovered}

    results = []
    errors = []
    with _progress_lock:
        for path in device_paths:
            if path not in _progress_states:
                dev = by_path.get(path)
                if dev is None:
                    errors.append({"path": path, "error": "device not found"})
                    continue
                _progress_states[path] = _disk.DevState(dev)

            state = _progress_states[path]
            _disk._poll(state)
            results.append(devstate_to_dict(state))

    terminal = {"done", "done_nostart", "failed", "lost"}
    all_done = all(r["status"] in terminal for r in results)
    any_started = any(r["ever_started"] for r in results)

    # Reap finished format processes
    for r in results:
        if r["status"] in terminal:
            with _format_procs_lock:
                proc = _format_procs.pop(r["path"], None)
            if proc is not None:
                try:
                    proc.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()

    return jsonify({
        "devices": results,
        "errors": errors,
        "all_done": all_done and any_started,
    })


# ---------------------------------------------------------------------------
# REST: Speed test (blocking — ~30s per disk)
# ---------------------------------------------------------------------------

@app.route("/api/speedtest", methods=["POST"])
@limiter.limit(_RATE_DESTRUCTIVE)
def api_speedtest():
    data = request.get_json()
    if not data or "devices" not in data:
        return jsonify({"error": "Missing 'devices' field"}), 400

    device_paths: list[str] = data["devices"]
    if not device_paths:
        return jsonify({"error": "No devices specified"}), 400

    discovered = _disk.discover()
    _disk.enrich_lsblk(discovered)
    by_path = {d.path: d for d in discovered}

    valid_paths = {d.path for d in discovered}
    results = []
    for path in device_paths:
        if path not in valid_paths:
            results.append({"device": path, "slot": "?", "serial": "?",
                            "speed": "error",
                            "reason": "device not found"})
            continue
        dev = by_path[path]
        slot = dev.slot
        serial = dev.serial or "?"

        try:
            r = subprocess.run(["blockdev", "--getsize64", path],
                               capture_output=True, text=True, timeout=30)
            if r.returncode != 0:
                raise ValueError(f"blockdev failed with rc={r.returncode}")
            size = int(r.stdout.strip())
        except Exception:
            size = 0

        if size == 0:
            results.append({
                "device": path,
                "slot": slot,
                "serial": serial,
                "speed": "no data",
            })
            continue

        try:
            r = subprocess.run(
                ["dd", f"if={path}", "of=/dev/null", "bs=4M", "count=256", "iflag=direct"],
                capture_output=True, text=True, timeout=60,
            )
            m_bytes = re.search(r"^(\d+) bytes", r.stderr, re.M)
            if m_bytes and int(m_bytes.group(1)) == 0:
                speed = "no data"
            else:
                m = re.search(r"([\d.]+)\s*(GB/s|MB/s|kB/s)", r.stderr)
                speed = f"{m.group(1)} {m.group(2)}" if m else "?"
        except subprocess.TimeoutExpired:
            speed = "timeout"
        except Exception:
            speed = "error"

        results.append({
            "device": path,
            "slot": slot,
            "serial": serial,
            "speed": speed,
        })

    _disk.log(
        "SPEEDTEST " + " ".join(
            f"serial={r.get('serial', '?')} dev={r.get('device', '?')} "
            f"slot={r.get('slot', '?')} read={r.get('speed', '?')}"
            for r in results
        )
    )
    return jsonify({"results": results})


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Disk Manager web server")
    parser.add_argument("--port", type=int, default=8880)
    parser.add_argument("--host", default="0.0.0.0")
    args = parser.parse_args()
    log.info(
        f"Starting Disk Manager web app on {args.host}:{args.port} "
        f"(disk log: {disk_logfile_path()})"
    )
    app.run(host=args.host, port=args.port, debug=False, threaded=True)
