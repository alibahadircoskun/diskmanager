from types import SimpleNamespace

import pytest

from web import app as web_app


@pytest.fixture(autouse=True)
def _reset_web_state():
    with web_app._progress_lock:
        web_app._progress_states.clear()
    with web_app._format_procs_lock:
        web_app._format_procs.clear()
    with web_app._format_jobs_lock:
        web_app._format_jobs.clear()
    web_app._polling_cancelled.clear()

    with web_app._health_cache_lock:
        web_app._health_cache_by_path.clear()
        web_app._health_cache_at = 0.0
    with web_app._zeroed_cache_lock:
        web_app._zeroed_cache.clear()

    yield

    with web_app._progress_lock:
        web_app._progress_states.clear()
    with web_app._format_procs_lock:
        web_app._format_procs.clear()
    with web_app._format_jobs_lock:
        web_app._format_jobs.clear()
    web_app._polling_cancelled.clear()
    with web_app._health_cache_lock:
        web_app._health_cache_by_path.clear()
        web_app._health_cache_at = 0.0
    with web_app._zeroed_cache_lock:
        web_app._zeroed_cache.clear()


@pytest.fixture
def client():
    web_app.app.config["TESTING"] = True
    with web_app.app.test_client() as c:
        yield c


def _fake_device(path: str, port: str, serial: str = "SERIAL1"):
    dev = web_app._disk.Device(path, port)
    dev.serial = serial
    dev.model = "MODELX"
    dev.size = "1.8T"
    dev.fmt_status = "512"
    dev.zeroed = ""
    dev.health = "?"
    return dev


def _write_test_log(path, count):
    lines = [f"2026-03-11 00:00:00,000 INFO line-{i}" for i in range(count)]
    payload = "\n".join(lines) + ("\n" if lines else "")
    path.write_text(payload, encoding="utf-8")
    return lines


def test_api_missing_skips_zeroed_probe_and_overlays_cache(monkeypatch, client):
    port = web_app._disk.PORTS[0]
    dev = _fake_device("/dev/sda", port, serial="S-001")
    calls = {"zeroed": 0}

    monkeypatch.setattr(web_app._disk, "discover", lambda: [dev])
    monkeypatch.setattr(web_app._disk, "enrich_lsblk", lambda devices: None)
    monkeypatch.setattr(web_app._disk, "enrich_fmt_status", lambda devices: None)

    def _count_zeroed(devices):
        calls["zeroed"] += 1

    monkeypatch.setattr(web_app._disk, "enrich_zeroed_status", _count_zeroed)
    web_app._store_zeroed_cache(dev.path, dev.serial, "zero")

    res = client.get("/api/missing")
    assert res.status_code == 200
    data = res.get_json()
    slot_row = next(s for s in data["slots"] if s["slot"] == dev.slot)
    assert slot_row["status"] == "PRESENT"
    assert slot_row["device"]["zeroed"] == "zero"
    assert calls["zeroed"] == 0


def test_api_health_returns_409_when_format_active(client):
    active_path = "/dev/sdu"
    with web_app._progress_lock:
        web_app._progress_states[active_path] = SimpleNamespace(status="formatting")

    res = client.post("/api/health", json={"devices": [active_path]})
    assert res.status_code == 409
    payload = res.get_json()
    assert payload["code"] == "format_active"
    assert active_path in payload["active_paths"]


def test_api_speedtest_returns_409_for_requested_active_path(client):
    active_path = "/dev/sdu"
    with web_app._progress_lock:
        web_app._progress_states[active_path] = SimpleNamespace(status="waiting")

    res = client.post("/api/speedtest", json={"devices": [active_path]})
    assert res.status_code == 409
    payload = res.get_json()
    assert payload["code"] == "format_active"
    assert active_path in payload["active_paths"]


def test_api_speedtest_returns_model_and_logs_incremental_rows(monkeypatch, client):
    dev_a = _fake_device("/dev/sda", web_app._disk.PORTS[0], serial="S-A")
    dev_b = _fake_device("/dev/sdb", web_app._disk.PORTS[1], serial="S-B")
    dev_a.model = "MODEL-A"
    dev_b.model = "MODEL-B"

    monkeypatch.setattr(web_app._disk, "discover", lambda: [dev_a, dev_b])
    monkeypatch.setattr(web_app._disk, "enrich_lsblk", lambda devices: None)

    logs = []
    monkeypatch.setattr(web_app._disk, "log", lambda msg: logs.append(msg))

    def fake_run(cmd, capture_output, text, timeout):
        if cmd[:2] == ["blockdev", "--getsize64"]:
            return SimpleNamespace(returncode=0, stdout="1099511627776\n", stderr="")
        if cmd and cmd[0] == "dd":
            dev_path = ""
            for token in cmd:
                if token.startswith("if="):
                    dev_path = token.split("=", 1)[1]
                    break
            speed = "160 MB/s" if dev_path == dev_a.path else "172 MB/s"
            return SimpleNamespace(
                returncode=0,
                stdout="",
                stderr=f"1073741824 bytes copied, 6.0 s, {speed}",
            )
        raise AssertionError(f"Unexpected command: {cmd}")

    monkeypatch.setattr(web_app.subprocess, "run", fake_run)

    res = client.post(
        "/api/speedtest",
        json={"devices": [dev_a.path, dev_b.path], "run_id": "run-123"},
    )
    assert res.status_code == 200
    payload = res.get_json()
    assert payload["run_id"] == "run-123"
    assert [r["device"] for r in payload["results"]] == [dev_a.path, dev_b.path]
    assert [r["serial"] for r in payload["results"]] == [dev_a.serial, dev_b.serial]
    assert [r["model"] for r in payload["results"]] == [dev_a.model, dev_b.model]
    assert [r["speed"] for r in payload["results"]] == ["160 MB/s", "172 MB/s"]
    assert len(logs) == 2
    assert all("SPEEDTEST run=run-123 " in line for line in logs)
    assert any(f"dev={dev_a.path}" in line for line in logs)
    assert any(f"dev={dev_b.path}" in line for line in logs)


def test_api_zeroed_scan_updates_cache_and_discover_overlay(monkeypatch, client):
    port = web_app._disk.PORTS[1]
    dev = _fake_device("/dev/sdb", port, serial="S-002")

    monkeypatch.setattr(web_app._disk, "discover", lambda: [dev])
    monkeypatch.setattr(web_app._disk, "enrich_lsblk", lambda devices: None)
    monkeypatch.setattr(web_app._disk, "enrich_fmt_status", lambda devices: None)
    monkeypatch.setattr(web_app._disk, "_is_disk_zeroed", lambda path: "data")

    scan = client.post("/api/zeroed/scan", json={"devices": [dev.path]})
    assert scan.status_code == 200
    scan_payload = scan.get_json()
    assert scan_payload["errors"] == []
    assert scan_payload["results"][0]["device"] == dev.path
    assert scan_payload["results"][0]["zeroed"] == "data"

    discover = client.get("/api/discover")
    assert discover.status_code == 200
    listed = discover.get_json()
    assert listed[0]["path"] == dev.path
    assert listed[0]["zeroed"] == "data"


def test_api_zeroed_scan_skips_when_requested_path_formatting(monkeypatch, client):
    port = web_app._disk.PORTS[2]
    dev = _fake_device("/dev/sdc", port, serial="S-003")
    monkeypatch.setattr(web_app._disk, "discover", lambda: [dev])
    monkeypatch.setattr(web_app._disk, "enrich_lsblk", lambda devices: None)

    with web_app._progress_lock:
        web_app._progress_states[dev.path] = SimpleNamespace(status="formatting")

    res = client.post("/api/zeroed/scan", json={"devices": [dev.path]})
    assert res.status_code == 200
    payload = res.get_json()
    assert payload["code"] == "format_active"
    assert payload["results"] == []
    assert len(payload["errors"]) == 1
    assert payload["errors"][0]["code"] == "format_active"
    assert payload["errors"][0]["path"] == dev.path
    assert dev.path in payload["blocked_paths"]


def test_api_zeroed_scan_returns_partial_for_mixed_formatting(monkeypatch, client):
    dev_a = _fake_device("/dev/sde", web_app._disk.PORTS[0], serial="S-101")
    dev_b = _fake_device("/dev/sdf", web_app._disk.PORTS[1], serial="S-102")
    monkeypatch.setattr(web_app._disk, "discover", lambda: [dev_a, dev_b])
    monkeypatch.setattr(web_app._disk, "enrich_lsblk", lambda devices: None)

    def fake_zeroed(path):
        return "zero" if path == dev_a.path else "data"

    monkeypatch.setattr(web_app._disk, "_is_disk_zeroed", fake_zeroed)

    with web_app._progress_lock:
        web_app._progress_states[dev_b.path] = SimpleNamespace(status="formatting")

    res = client.post("/api/zeroed/scan", json={"devices": [dev_a.path, dev_b.path]})
    assert res.status_code == 200
    payload = res.get_json()
    assert payload["code"] == "format_active"
    assert dev_b.path in payload["blocked_paths"]
    assert len(payload["results"]) == 1
    assert payload["results"][0]["device"] == dev_a.path
    assert payload["results"][0]["zeroed"] == "zero"
    assert len(payload["errors"]) == 1
    assert payload["errors"][0]["path"] == dev_b.path
    assert payload["errors"][0]["code"] == "format_active"


def test_format_start_invalidates_zeroed_cache(monkeypatch, client):
    port = web_app._disk.PORTS[3]
    dev = _fake_device("/dev/sdd", port, serial="S-004")

    monkeypatch.setattr(web_app._disk, "discover", lambda: [dev])
    monkeypatch.setattr(web_app._disk, "enrich_lsblk", lambda devices: None)
    monkeypatch.setattr(web_app._disk, "prep_for_format", lambda path: None)
    monkeypatch.setattr(web_app._disk, "sg_device", lambda path: "/dev/sg-test")
    monkeypatch.setattr(web_app._disk, "log", lambda msg: None)
    monkeypatch.setattr(web_app.Path, "is_block_device", lambda self: True)

    class _FakeProc:
        def poll(self):
            return None

    monkeypatch.setattr(web_app.subprocess, "Popen", lambda *args, **kwargs: _FakeProc())
    monkeypatch.setattr(web_app.time, "sleep", lambda *_: None)

    web_app._store_zeroed_cache(dev.path, dev.serial, "zero")
    with web_app._zeroed_cache_lock:
        assert len(web_app._zeroed_cache) == 1

    res = client.post("/api/format/start", json={"devices": [dev.path]})
    assert res.status_code == 200
    payload = res.get_json()
    assert dev.path in payload["started"]

    with web_app._zeroed_cache_lock:
        assert len(web_app._zeroed_cache) == 0


def test_api_logs_default_limit_returns_pagination_metadata(monkeypatch, tmp_path, client):
    log_path = tmp_path / "diskops.log"
    all_lines = _write_test_log(log_path, 620)
    monkeypatch.setattr(web_app, "disk_logfile_path", lambda: str(log_path))

    res = client.get("/api/logs")
    assert res.status_code == 200
    payload = res.get_json()

    assert payload["ok"] is True
    assert payload["source"] == str(log_path)
    assert payload["limit"] == 500
    assert payload["offset"] == 0
    assert payload["returned_count"] == 500
    assert payload["line_count"] == 500
    assert payload["has_more"] is True
    assert payload["next_offset"] == 500
    assert payload["lines"] == all_lines[-500:]


def test_api_logs_limit_offset_pagination_windows_do_not_overlap(monkeypatch, tmp_path, client):
    log_path = tmp_path / "diskops.log"
    all_lines = _write_test_log(log_path, 620)
    monkeypatch.setattr(web_app, "disk_logfile_path", lambda: str(log_path))

    page1 = client.get("/api/logs?limit=200&offset=0").get_json()
    page2 = client.get("/api/logs?limit=200&offset=200").get_json()
    page3 = client.get("/api/logs?limit=200&offset=400").get_json()
    page4 = client.get("/api/logs?limit=200&offset=600").get_json()

    assert page1["lines"] == all_lines[420:620]
    assert page2["lines"] == all_lines[220:420]
    assert page3["lines"] == all_lines[20:220]
    assert page4["lines"] == all_lines[0:20]

    assert page1["has_more"] is True and page1["next_offset"] == 200
    assert page2["has_more"] is True and page2["next_offset"] == 400
    assert page3["has_more"] is True and page3["next_offset"] == 600
    assert page4["has_more"] is False and page4["next_offset"] is None

    assert set(page1["lines"]).isdisjoint(set(page2["lines"]))
    assert set(page2["lines"]).isdisjoint(set(page3["lines"]))
    assert set(page3["lines"]).isdisjoint(set(page4["lines"]))


@pytest.mark.parametrize(
    "query",
    [
        "limit=0",
        "limit=5001",
        "limit=notanint",
        "offset=-1",
        "offset=notanint",
    ],
)
def test_api_logs_rejects_invalid_limit_offset(monkeypatch, tmp_path, client, query):
    log_path = tmp_path / "diskops.log"
    _write_test_log(log_path, 10)
    monkeypatch.setattr(web_app, "disk_logfile_path", lambda: str(log_path))

    res = client.get(f"/api/logs?{query}")
    assert res.status_code == 400
    payload = res.get_json()
    assert payload["ok"] is False
    assert "Invalid" in payload["error"]
    assert payload["source"] == str(log_path)


def test_api_logs_has_more_next_offset_boundary(monkeypatch, tmp_path, client):
    log_path = tmp_path / "diskops.log"
    all_lines = _write_test_log(log_path, 3)
    monkeypatch.setattr(web_app, "disk_logfile_path", lambda: str(log_path))

    first = client.get("/api/logs?limit=2&offset=0").get_json()
    second = client.get("/api/logs?limit=2&offset=2").get_json()
    third = client.get("/api/logs?limit=2&offset=3").get_json()

    assert first["lines"] == all_lines[1:3]
    assert first["returned_count"] == 2
    assert first["has_more"] is True
    assert first["next_offset"] == 2

    assert second["lines"] == all_lines[0:1]
    assert second["returned_count"] == 1
    assert second["has_more"] is False
    assert second["next_offset"] is None

    assert third["lines"] == []
    assert third["returned_count"] == 0
    assert third["has_more"] is False
    assert third["next_offset"] is None


def test_api_logs_delete_selected_lines_from_tail_occurrences(monkeypatch, tmp_path, client):
    log_path = tmp_path / "diskops.log"
    lines = [
        "2026-03-11 00:00:00,000 INFO alpha",
        "2026-03-11 00:00:01,000 INFO beta",
        "2026-03-11 00:00:02,000 INFO alpha",
        "2026-03-11 00:00:03,000 INFO gamma",
    ]
    log_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    monkeypatch.setattr(web_app, "disk_logfile_path", lambda: str(log_path))

    res = client.post("/api/logs/delete", json={"lines": [lines[2], lines[3]]})
    assert res.status_code == 200
    payload = res.get_json()
    assert payload["ok"] is True
    assert payload["removed_count"] == 2
    assert payload["remaining_count"] == 2
    assert payload["source"] == str(log_path)

    remaining = log_path.read_text(encoding="utf-8").splitlines()
    assert remaining == [lines[0], lines[1]]


def test_api_logs_delete_rejects_invalid_payload(monkeypatch, tmp_path, client):
    log_path = tmp_path / "diskops.log"
    _write_test_log(log_path, 5)
    monkeypatch.setattr(web_app, "disk_logfile_path", lambda: str(log_path))

    res = client.post("/api/logs/delete", json={"lines": []})
    assert res.status_code == 400
    payload = res.get_json()
    assert payload["ok"] is False
    assert "Missing 'lines' list." in payload["error"]


def test_api_logs_delete_returns_404_when_log_missing(monkeypatch, tmp_path, client):
    log_path = tmp_path / "missing.log"
    monkeypatch.setattr(web_app, "disk_logfile_path", lambda: str(log_path))

    res = client.post("/api/logs/delete", json={"lines": ["line"]})
    assert res.status_code == 404
    payload = res.get_json()
    assert payload["ok"] is False
    assert payload["source"] == str(log_path)
    assert payload["error"] == "Log file not found."
