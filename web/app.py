#!/usr/bin/env python3
"""app.py — Flask web backend for disk management tool."""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime
from collections import Counter
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
_HEALTH_SCAN_TIMEOUT_SEC = max(15, int(os.environ.get("HEALTH_SCAN_TIMEOUT_SEC", "90")))
_HEALTH_CACHE_TTL_SEC    = max(0, int(os.environ.get("HEALTH_CACHE_TTL_SEC", "20")))
_FORMAT_DETECT_TIMEOUT_SEC = max(2, int(os.environ.get("FORMAT_DETECT_TIMEOUT_SEC", "4")))
_LOGS_DEFAULT_LIMIT = 500
_LOGS_MAX_LIMIT = 5000

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

# Single-flight health scan protection + short-lived cache.
_health_scan_lock = threading.Lock()
_health_cache_lock = threading.Lock()
_health_cache_at = 0.0
_health_cache_by_path: dict[str, str] = {}

# Zeroed scan cache for on-demand probes.
_zeroed_cache_lock = threading.Lock()
_zeroed_cache: dict[str, dict[str, object]] = {}

_TERMINAL_FORMAT_STATUSES = {"done", "done_nostart", "failed", "lost"}
_FORMAT_IN_PROGRESS_RE = re.compile(
    r"progress indication|format(?:\s+command)?\s+in\s+progress|in progress.*format|not ready.*format",
    re.I,
)

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


def _get_devices_enriched(*, include_zeroed: bool = False) -> list[_disk.Device]:
    devices = _disk.discover()
    if devices:
        _disk.enrich_lsblk(devices)
        _disk.enrich_fmt_status(devices)
        if include_zeroed:
            _disk.enrich_zeroed_status(devices)
        _apply_zeroed_cache(devices)
    return devices


def _active_format_paths() -> set[str]:
    active: set[str] = set()

    with _progress_lock:
        for path, state in _progress_states.items():
            if state.status not in _TERMINAL_FORMAT_STATUSES:
                active.add(path)

    stale: list[str] = []
    with _format_procs_lock:
        for path, proc in _format_procs.items():
            if proc.poll() is None:
                active.add(path)
            else:
                stale.append(path)
        for path in stale:
            _format_procs.pop(path, None)

    return active


def _format_conflict_response(active_paths: set[str]):
    return jsonify({
        "error": "Scan blocked while format is active. Wait for monitor completion and retry.",
        "code": "format_active",
        "active_paths": sorted(active_paths),
    }), 409


def _path_reports_format_in_progress(path: str) -> bool:
    sg_path = _disk.sg_device(path)
    if not sg_path:
        return False

    outputs: list[str] = []
    for cmd in (
        ["sg_requests", sg_path],
        ["sg_requests", "--progress", sg_path],
    ):
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=_FORMAT_DETECT_TIMEOUT_SEC)
            payload = (r.stdout or "") + "\n" + (r.stderr or "")
            if payload.strip():
                outputs.append(payload)
        except (subprocess.TimeoutExpired, OSError):
            continue

    if not outputs:
        return False

    return bool(_FORMAT_IN_PROGRESS_RE.search("\n".join(outputs)))


def _zeroed_cache_key(path: str, serial: str) -> str:
    serial_norm = (serial or "").strip()
    return f"{serial_norm}|{path}"


def _store_zeroed_cache(path: str, serial: str, zeroed: str) -> None:
    normalized = zeroed if zeroed in {"zero", "data"} else ""
    entry = {
        "path": path,
        "serial": (serial or "").strip(),
        "zeroed": normalized,
        "at": time.time(),
    }
    key = _zeroed_cache_key(path, serial)
    with _zeroed_cache_lock:
        _zeroed_cache[key] = entry


def _invalidate_zeroed_cache(path: str, serial: str) -> None:
    serial_norm = (serial or "").strip()
    with _zeroed_cache_lock:
        drop_keys = [
            k for k, entry in _zeroed_cache.items()
            if entry.get("path") == path or (serial_norm and entry.get("serial") == serial_norm)
        ]
        for key in drop_keys:
            _zeroed_cache.pop(key, None)


def _apply_zeroed_cache(devices: list[_disk.Device]) -> None:
    with _zeroed_cache_lock:
        entries = list(_zeroed_cache.values())

    by_serial = {
        str(entry.get("serial", "")).strip(): str(entry.get("zeroed", "")).strip()
        for entry in entries
        if str(entry.get("serial", "")).strip()
    }
    by_path = {
        str(entry.get("path", "")).strip(): str(entry.get("zeroed", "")).strip()
        for entry in entries
        if str(entry.get("path", "")).strip()
    }

    for dev in devices:
        serial = (dev.serial or "").strip()
        if serial and serial in by_serial:
            dev.zeroed = by_serial[serial]
            continue
        cached = by_path.get(dev.path)
        if cached is not None:
            dev.zeroed = cached


def _apply_health_cache(devices: list[_disk.Device]) -> tuple[int, float]:
    """Apply cached health values to devices, returning (hits, cache_age_sec)."""
    with _health_cache_lock:
        if not _health_cache_by_path:
            return 0, 0.0
        cache = dict(_health_cache_by_path)
        age = max(0.0, time.monotonic() - _health_cache_at)

    hits = 0
    for dev in devices:
        cached = cache.get(dev.path)
        if cached is None:
            continue
        dev.health = cached
        hits += 1
    return hits, age


def _store_health_cache(devices: list[_disk.Device]) -> None:
    global _health_cache_at, _health_cache_by_path
    snapshot = {d.path: d.health for d in devices if d.path}
    with _health_cache_lock:
        _health_cache_by_path = snapshot
        _health_cache_at = time.monotonic()


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


def tail_log_window(path: Path, limit: int, offset: int) -> tuple[list[str], bool]:
    """Return one paged log window and whether older lines still exist."""
    if limit <= 0:
        return [], False
    if offset < 0:
        return [], False

    required = limit + offset
    probe = tail_log_lines(path, limit=required + 1)
    has_more = len(probe) > required

    pool = probe[-required:] if has_more and required > 0 else probe
    if offset >= len(pool):
        return [], has_more

    if offset > 0:
        pool = pool[:-offset]

    return pool[-limit:], has_more


def parse_logs_pagination() -> tuple[int, int]:
    raw_limit = request.args.get("limit", str(_LOGS_DEFAULT_LIMIT))
    raw_offset = request.args.get("offset", "0")

    try:
        limit = int(raw_limit)
    except (TypeError, ValueError):
        raise ValueError(f"Invalid 'limit': expected integer between 1 and {_LOGS_MAX_LIMIT}.")
    try:
        offset = int(raw_offset)
    except (TypeError, ValueError):
        raise ValueError("Invalid 'offset': expected integer >= 0.")

    if limit < 1 or limit > _LOGS_MAX_LIMIT:
        raise ValueError(f"Invalid 'limit': expected integer between 1 and {_LOGS_MAX_LIMIT}.")
    if offset < 0:
        raise ValueError("Invalid 'offset': expected integer >= 0.")

    return limit, offset


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
        limit, offset = parse_logs_pagination()
        lines, has_more = tail_log_window(path, limit=limit, offset=offset)
        returned_count = len(lines)
        return jsonify({
            "ok": True,
            "source": source,
            "line_count": returned_count,
            "limit": limit,
            "offset": offset,
            "returned_count": returned_count,
            "has_more": has_more,
            "next_offset": (offset + returned_count) if has_more else None,
            "lines": lines,
            "fetched_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        })
    except ValueError as e:
        return jsonify({
            "ok": False,
            "source": source,
            "error": str(e),
        }), 400
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


@app.route("/api/logs/delete", methods=["POST"])
@limiter.limit(_RATE_DESTRUCTIVE)
def api_logs_delete():
    source = disk_logfile_path()
    path = Path(source)
    data = request.get_json(silent=True) or {}
    raw_lines = data.get("lines")
    if not isinstance(raw_lines, list) or not raw_lines:
        return jsonify({
            "ok": False,
            "source": source,
            "error": "Missing 'lines' list.",
        }), 400

    selected = [str(line) for line in raw_lines if line is not None]
    if not selected:
        return jsonify({
            "ok": False,
            "source": source,
            "error": "No valid log lines provided.",
        }), 400

    try:
        payload = path.read_text(encoding="utf-8")
        had_trailing_newline = payload.endswith("\n")
        existing_lines = payload.splitlines()

        delete_counts = Counter(selected)
        requested_count = sum(delete_counts.values())

        kept_reversed: list[str] = []
        removed_count = 0
        for line in reversed(existing_lines):
            if delete_counts.get(line, 0) > 0:
                delete_counts[line] -= 1
                removed_count += 1
                continue
            kept_reversed.append(line)

        kept_lines = list(reversed(kept_reversed))
        next_payload = "\n".join(kept_lines)
        if had_trailing_newline and kept_lines:
            next_payload += "\n"

        path.write_text(next_payload, encoding="utf-8")
        return jsonify({
            "ok": True,
            "source": source,
            "requested_count": requested_count,
            "removed_count": removed_count,
            "remaining_count": len(kept_lines),
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
            "error": "Log file is unreadable or not writable (permission denied).",
        }), 403
    except OSError as e:
        return jsonify({
            "ok": False,
            "source": source,
            "error": f"Failed to update log file: {e}",
        }), 500


# ---------------------------------------------------------------------------
# REST: Discover & Missing
# ---------------------------------------------------------------------------

@app.route("/api/discover")
def api_discover():
    try:
        devices = _get_devices_enriched(include_zeroed=False)
        return jsonify([device_to_dict(d) for d in devices])
    except Exception as e:
        log.exception("discover failed")
        return jsonify({"error": str(e)}), 500


@app.route("/api/missing")
def api_missing():
    try:
        devices = _get_devices_enriched(include_zeroed=False)
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
    data = request.get_json(silent=True) or {}
    raw_paths = data.get("devices", [])
    if raw_paths is None:
        raw_paths = []
    if not isinstance(raw_paths, list):
        return jsonify({"error": "Field 'devices' must be a list when provided."}), 400
    requested_paths = {str(p).strip() for p in raw_paths if str(p).strip()}

    try:
        active_paths = _active_format_paths()
        if active_paths and (not requested_paths or active_paths.intersection(requested_paths)):
            return _format_conflict_response(active_paths if not requested_paths else active_paths.intersection(requested_paths))

        devices = _disk.discover()
        if requested_paths:
            devices = [d for d in devices if d.path in requested_paths]
        if not devices:
            return jsonify({"devices": [], "rc": 0, "elapsed": 0,
                            "message": "No disks found for requested selection."})

        _disk.enrich_lsblk(devices)

        # Fast-path: return very recent cache to avoid repeated long scans.
        cached_hits, cache_age = _apply_health_cache(devices)
        if (_HEALTH_CACHE_TTL_SEC > 0 and cached_hits == len(devices)
                and cache_age <= _HEALTH_CACHE_TTL_SEC):
            return jsonify({
                "devices": [device_to_dict(d) for d in devices],
                "rc": 0,
                "elapsed": 0,
                "cached": True,
                "cache_age": round(cache_age, 1),
                "message": f"Using cached health from {cache_age:.1f}s ago.",
            })

        if not _health_scan_lock.acquire(blocking=False):
            cached_hits, cache_age = _apply_health_cache(devices)
            if cached_hits == len(devices) and cached_hits > 0:
                return jsonify({
                    "devices": [device_to_dict(d) for d in devices],
                    "rc": 0,
                    "elapsed": 0,
                    "cached": True,
                    "in_progress": True,
                    "cache_age": round(cache_age, 1),
                    "message": f"Health scan already running; showing cached data from {cache_age:.1f}s ago.",
                })
            return jsonify({"error": "Health scan already running. Please retry shortly."}), 409

        try:
            if not Path(_disk.HDSENTINEL).exists():
                return jsonify({"error": f"HDSentinel not found at {_disk.HDSENTINEL}"}), 500

            devlist = ",".join(d.port for d in devices)
            cmd = [_disk.HDSENTINEL, "-onlydevs", devlist, "-dump"]

            log.info("health scan started: devices=%d timeout=%ss", len(devices), _HEALTH_SCAN_TIMEOUT_SEC)
            start = time.monotonic()
            timed_out = False
            out = ""
            rc = 0

            try:
                proc = subprocess.run(cmd, capture_output=True, timeout=_HEALTH_SCAN_TIMEOUT_SEC)
                rc = proc.returncode
                out = proc.stdout.decode("utf-8", errors="replace")
            except subprocess.TimeoutExpired as exc:
                timed_out = True
                rc = 124
                payload = exc.stdout or b""
                if isinstance(payload, bytes):
                    out = payload.decode("utf-8", errors="replace")
                else:
                    out = str(payload)

            elapsed = time.monotonic() - start
            _disk._parse_hdsentinel_health(out, devices)
            _store_health_cache(devices)

            for d in devices:
                _disk.log(f"HEALTH slot={d.slot} dev={d.path} model={d.model} serial={d.serial} health={d.health}%")

            resp = {
                "devices": [device_to_dict(d) for d in devices],
                "rc": rc,
                "elapsed": round(elapsed, 1),
            }
            if timed_out:
                resp["timeout"] = True
                resp["message"] = (
                    f"HDSentinel reached timeout after {_HEALTH_SCAN_TIMEOUT_SEC}s. "
                    "Partial health results shown."
                )
            log.info("health scan finished: devices=%d elapsed=%.1fs rc=%d timed_out=%s",
                     len(devices), elapsed, rc, timed_out)
            return jsonify(resp)
        finally:
            _health_scan_lock.release()
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
                _invalidate_zeroed_cache(path, dev.serial)
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
# REST: Zeroed status probe (on-demand only)
# ---------------------------------------------------------------------------

@app.route("/api/zeroed/scan", methods=["POST"])
@limiter.limit(_RATE_DESTRUCTIVE)
def api_zeroed_scan():
    data = request.get_json(silent=True) or {}
    raw_paths = data.get("devices", [])
    if not isinstance(raw_paths, list):
        return jsonify({"error": "Missing 'devices' list"}), 400

    requested_paths: list[str] = []
    seen: set[str] = set()
    for item in raw_paths:
        path = str(item).strip()
        if not path or path in seen:
            continue
        seen.add(path)
        requested_paths.append(path)
    if not requested_paths:
        return jsonify({"error": "No devices specified"}), 400

    active_paths = _active_format_paths()
    blocked_paths = active_paths.intersection(set(requested_paths))

    discovered = _disk.discover()
    _disk.enrich_lsblk(discovered)
    by_path = {d.path: d for d in discovered}

    results = []
    errors = []
    valid_paths: list[str] = []

    for path in requested_paths:
        dev = by_path.get(path)
        if dev is None:
            errors.append({"path": path, "error": "device not found"})
            continue
        valid_paths.append(path)

    # Detect formatting directly from sg_requests to catch runs not launched by this service.
    externally_active: set[str] = set()
    probe_candidates = [path for path in valid_paths if path not in blocked_paths]
    if probe_candidates:
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(probe_candidates), 8)) as ex:
            fut_to_path = {ex.submit(_path_reports_format_in_progress, path): path for path in probe_candidates}
            for fut, path in fut_to_path.items():
                try:
                    if fut.result():
                        externally_active.add(path)
                except Exception:
                    # Detection failure should not abort request; proceed to scan path.
                    pass
    blocked_paths.update(externally_active)

    if blocked_paths:
        for path in requested_paths:
            if path in blocked_paths and path in by_path:
                errors.append({
                    "path": path,
                    "error": "format active",
                    "code": "format_active",
                })

    def _scan_one(path: str) -> dict:
        dev = by_path[path]
        started = time.monotonic()
        reason = ""
        zeroed_raw = ""
        try:
            zeroed_raw = _disk._is_disk_zeroed(path)
            if zeroed_raw not in {"zero", "data", ""}:
                zeroed_raw = ""
        except Exception as exc:
            reason = str(exc)

        elapsed = round(time.monotonic() - started, 1)
        normalized = zeroed_raw if zeroed_raw in {"zero", "data"} else "unknown"
        if normalized == "unknown" and not reason:
            reason = "probe unavailable"

        _store_zeroed_cache(path, dev.serial, normalized)

        payload: dict[str, object] = {
            "device": path,
            "slot": dev.slot,
            "serial": dev.serial or "?",
            "zeroed": normalized,
            "elapsed": elapsed,
        }
        if reason:
            payload["reason"] = reason
        return payload

    scannable_paths = [path for path in valid_paths if path not in blocked_paths]
    if scannable_paths:
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(scannable_paths), 8)) as ex:
            # Keep response order aligned with requested paths.
            for payload in ex.map(_scan_one, scannable_paths):
                results.append(payload)

    response = {
        "results": results,
        "errors": errors,
    }
    if blocked_paths:
        response["code"] = "format_active"
        response["blocked_paths"] = sorted(blocked_paths)
    return jsonify(response)


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
    run_id = str(data.get("run_id", "")).strip() or uuid.uuid4().hex[:12]

    requested_paths = {str(p).strip() for p in device_paths if str(p).strip()}
    active_paths = _active_format_paths()
    blocked_paths = active_paths.intersection(requested_paths)
    if blocked_paths:
        return _format_conflict_response(blocked_paths)

    discovered = _disk.discover()
    _disk.enrich_lsblk(discovered)
    by_path = {d.path: d for d in discovered}

    valid_paths = {d.path for d in discovered}
    results = []

    def _append_speed_result(row: dict) -> None:
        results.append(row)
        _disk.log(
            "SPEEDTEST "
            f"run={run_id} "
            f"serial={row.get('serial', '?')} "
            f"dev={row.get('device', '?')} "
            f"slot={row.get('slot', '?')} "
            f"read={row.get('speed', '?')}"
        )

    for path in device_paths:
        if path not in valid_paths:
            _append_speed_result({
                "device": path,
                "slot": "?",
                "serial": "?",
                "model": "?",
                "speed": "error",
                "reason": "device not found",
            })
            continue
        dev = by_path[path]
        slot = dev.slot
        serial = dev.serial or "?"
        model = dev.model or "?"

        try:
            r = subprocess.run(["blockdev", "--getsize64", path],
                               capture_output=True, text=True, timeout=30)
            if r.returncode != 0:
                raise ValueError(f"blockdev failed with rc={r.returncode}")
            size = int(r.stdout.strip())
        except Exception:
            size = 0

        if size == 0:
            _append_speed_result({
                "device": path,
                "slot": slot,
                "serial": serial,
                "model": model,
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

        _append_speed_result({
            "device": path,
            "slot": slot,
            "serial": serial,
            "model": model,
            "speed": speed,
        })
    return jsonify({"run_id": run_id, "results": results})


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
