import json
import logging
from types import SimpleNamespace

import pytest

import disk


@pytest.fixture(autouse=True)
def _reset_inventory_cache():
    disk._inventory_cache = {}
    disk._inventory_cache_mtime = None
    disk._inventory_warning_key = None
    yield
    disk._inventory_cache = {}
    disk._inventory_cache_mtime = None
    disk._inventory_warning_key = None


def _write_inventory(monkeypatch, tmp_path, rows):
    path = tmp_path / "components_2026-03-11.json"
    path.write_text(json.dumps(rows), encoding="utf-8")
    monkeypatch.setattr(disk, "INVENTORY_JSON", path)
    return path


def _fake_lsblk_output(serial: str, model: str = "RAW-MODEL", name: str = "sda") -> str:
    return f'NAME="{name}" SIZE="1.8T" SERIAL="{serial}" MODEL="{model}"\n'


def test_inventory_serial_match_overrides_model_case_insensitive(monkeypatch, tmp_path):
    _write_inventory(
        monkeypatch,
        tmp_path,
        [{"Name": "Mapped Model", "Serial": "ABC123", "Category": "SAS Disk"}],
    )

    dev = disk.Device("/dev/sda", "")

    def fake_run(cmd, **kwargs):
        if cmd[:2] == ["lsblk", "-dn"]:
            return SimpleNamespace(returncode=0, stdout=_fake_lsblk_output("abc123", "ORIG"), stderr="")
        raise AssertionError(f"Unexpected command: {cmd}")

    monkeypatch.setattr(disk.subprocess, "run", fake_run)
    monkeypatch.setattr(disk.shutil, "which", lambda _: None)

    disk.enrich_lsblk([dev])

    assert dev.serial == "abc123"
    assert dev.model == "Mapped Model"


def test_inventory_no_match_keeps_existing_model(monkeypatch, tmp_path):
    _write_inventory(
        monkeypatch,
        tmp_path,
        [{"Name": "Other Model", "Serial": "OTHER001", "Category": "SAS Disk"}],
    )

    dev = disk.Device("/dev/sda", "")

    def fake_run(cmd, **kwargs):
        if cmd[:2] == ["lsblk", "-dn"]:
            return SimpleNamespace(returncode=0, stdout=_fake_lsblk_output("ABC123", "RAWMODEL"), stderr="")
        raise AssertionError(f"Unexpected command: {cmd}")

    monkeypatch.setattr(disk.subprocess, "run", fake_run)
    monkeypatch.setattr(disk.shutil, "which", lambda _: None)

    disk.enrich_lsblk([dev])

    assert dev.model == "RAWMODEL"


def test_inventory_name_decodes_html_entities(monkeypatch, tmp_path):
    _write_inventory(
        monkeypatch,
        tmp_path,
        [{"Name": "DELL 500GB 7.2K SAS 2.5&quot;", "Serial": "ABC123", "Category": "SAS Disk"}],
    )

    dev = disk.Device("/dev/sda", "")

    def fake_run(cmd, **kwargs):
        if cmd[:2] == ["lsblk", "-dn"]:
            return SimpleNamespace(returncode=0, stdout=_fake_lsblk_output("ABC123", "RAWMODEL"), stderr="")
        raise AssertionError(f"Unexpected command: {cmd}")

    monkeypatch.setattr(disk.subprocess, "run", fake_run)
    monkeypatch.setattr(disk.shutil, "which", lambda _: None)

    disk.enrich_lsblk([dev])

    assert dev.model == 'DELL 500GB 7.2K SAS 2.5"'


def test_inventory_non_disk_category_is_ignored(monkeypatch, tmp_path):
    _write_inventory(
        monkeypatch,
        tmp_path,
        [{"Name": "NIC Name", "Serial": "ABC123", "Category": "Fiber NIC"}],
    )

    dev = disk.Device("/dev/sda", "")

    def fake_run(cmd, **kwargs):
        if cmd[:2] == ["lsblk", "-dn"]:
            return SimpleNamespace(returncode=0, stdout=_fake_lsblk_output("ABC123", "RAWMODEL"), stderr="")
        raise AssertionError(f"Unexpected command: {cmd}")

    monkeypatch.setattr(disk.subprocess, "run", fake_run)
    monkeypatch.setattr(disk.shutil, "which", lambda _: None)

    disk.enrich_lsblk([dev])

    assert dev.model == "RAWMODEL"


def test_inventory_uses_smartctl_fallback_serial(monkeypatch, tmp_path):
    _write_inventory(
        monkeypatch,
        tmp_path,
        [{"Name": "Smartctl Model", "Serial": "ABC999", "Category": "SSD Disk"}],
    )

    dev = disk.Device("/dev/sda", "")

    def fake_run(cmd, **kwargs):
        if cmd[:2] == ["lsblk", "-dn"]:
            return SimpleNamespace(returncode=0, stdout=_fake_lsblk_output("", "RAWMODEL"), stderr="")
        if cmd[:2] == ["smartctl", "-i"]:
            return SimpleNamespace(returncode=0, stdout="Serial number: ABC999\n", stderr="")
        raise AssertionError(f"Unexpected command: {cmd}")

    monkeypatch.setattr(disk.subprocess, "run", fake_run)
    monkeypatch.setattr(disk.shutil, "which", lambda cmd: "/usr/bin/smartctl" if cmd == "smartctl" else None)

    disk.enrich_lsblk([dev])

    assert dev.serial == "ABC999"
    assert dev.model == "Smartctl Model"


def test_inventory_missing_file_keeps_model(monkeypatch, tmp_path):
    missing_path = tmp_path / "missing-components.json"
    monkeypatch.setattr(disk, "INVENTORY_JSON", missing_path)

    dev = disk.Device("/dev/sda", "")

    def fake_run(cmd, **kwargs):
        if cmd[:2] == ["lsblk", "-dn"]:
            return SimpleNamespace(returncode=0, stdout=_fake_lsblk_output("ABC123", "RAWMODEL"), stderr="")
        raise AssertionError(f"Unexpected command: {cmd}")

    monkeypatch.setattr(disk.subprocess, "run", fake_run)
    monkeypatch.setattr(disk.shutil, "which", lambda _: None)

    disk.enrich_lsblk([dev])

    assert dev.model == "RAWMODEL"


def test_inventory_invalid_json_keeps_model(monkeypatch, tmp_path):
    invalid_path = tmp_path / "components_2026-03-11.json"
    invalid_path.write_text("{not-json", encoding="utf-8")
    monkeypatch.setattr(disk, "INVENTORY_JSON", invalid_path)

    dev = disk.Device("/dev/sda", "")

    def fake_run(cmd, **kwargs):
        if cmd[:2] == ["lsblk", "-dn"]:
            return SimpleNamespace(returncode=0, stdout=_fake_lsblk_output("ABC123", "RAWMODEL"), stderr="")
        raise AssertionError(f"Unexpected command: {cmd}")

    monkeypatch.setattr(disk.subprocess, "run", fake_run)
    monkeypatch.setattr(disk.shutil, "which", lambda _: None)

    disk.enrich_lsblk([dev])

    assert dev.model == "RAWMODEL"


def test_inventory_duplicate_serial_keeps_first_and_logs_warning(monkeypatch, tmp_path, caplog):
    _write_inventory(
        monkeypatch,
        tmp_path,
        [
            {"Name": "First Name", "Serial": "DUP001", "Category": "SAS Disk"},
            {"Name": "Second Name", "Serial": "DUP001", "Category": "SAS Disk"},
        ],
    )

    dev = disk.Device("/dev/sda", "")

    def fake_run(cmd, **kwargs):
        if cmd[:2] == ["lsblk", "-dn"]:
            return SimpleNamespace(returncode=0, stdout=_fake_lsblk_output("DUP001", "RAWMODEL"), stderr="")
        raise AssertionError(f"Unexpected command: {cmd}")

    monkeypatch.setattr(disk.subprocess, "run", fake_run)
    monkeypatch.setattr(disk.shutil, "which", lambda _: None)
    caplog.set_level(logging.WARNING, logger="diskops")

    disk.enrich_lsblk([dev])

    assert dev.model == "First Name"
    assert any("duplicate serial DUP001" in rec.message for rec in caplog.records)
