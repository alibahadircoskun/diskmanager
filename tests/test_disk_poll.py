import time

import disk


def test_poll_waiting_timeout_does_not_probe_zeroed(monkeypatch, tmp_path):
    fake_dev_path = tmp_path / "fake-block"
    fake_dev_path.write_bytes(b"x")

    dev = disk.Device(str(fake_dev_path), disk.PORTS[0])
    dev.serial = "TST123"
    dev.model = "TEST-MODEL"
    state = disk.DevState(dev)
    state.status = "waiting"
    state.ever_started = False
    state.start = time.monotonic() - 31

    def fake_fmt_status(devices):
        for item in devices:
            item.fmt_status = "512"

    monkeypatch.setattr(disk, "enrich_fmt_status", fake_fmt_status)

    def fail_zeroed(devices):
        raise AssertionError("enrich_zeroed_status should not be called from _poll timeout fallback")

    monkeypatch.setattr(disk, "enrich_zeroed_status", fail_zeroed)
    monkeypatch.setattr(disk, "log", lambda msg: None)

    disk._poll(state)

    assert state.status == "done_nostart"
    assert state.progress == 100.0
    assert state.eta == "done"

