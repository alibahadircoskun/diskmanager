from types import SimpleNamespace

import disk


def test_all_terminal_true_when_no_device_ever_started():
    states = [
        SimpleNamespace(status="failed", ever_started=False),
        SimpleNamespace(status="lost", ever_started=False),
    ]

    assert disk._all_terminal(states) is True


def test_all_terminal_false_when_waiting_device_remains():
    states = [
        SimpleNamespace(status="done_nostart", ever_started=True),
        SimpleNamespace(status="waiting", ever_started=False),
    ]

    assert disk._all_terminal(states) is False
