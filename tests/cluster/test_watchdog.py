"""Tests for the stop watchdog thread (no NATS needed)."""

from collections.abc import Generator
import logging
import threading
import time
from unittest.mock import patch

import pytest

from homeassistant.cluster import watchdog
from homeassistant.cluster.watchdog import StopWatchdog
from homeassistant.const import RESTART_EXIT_CODE


@pytest.fixture
def exits() -> Generator[list[int]]:
    """Record ``os._exit`` calls of the watchdog."""
    calls: list[int] = []
    with patch.object(watchdog.os, "_exit", calls.append):
        yield calls


@pytest.fixture
def dog() -> Generator[StopWatchdog]:
    """A started watchdog, always stopped at the end."""
    dog = StopWatchdog("test")
    dog.start()
    yield dog
    dog.stop()


def test_fires_once_after_the_deadline(dog: StopWatchdog, exits: list[int]) -> None:
    """The process is ended once, with the restart exit code."""
    dog.arm(time.monotonic() + 0.2)
    assert exits == []
    time.sleep(0.6)
    assert exits == [RESTART_EXIT_CODE]
    assert not dog.armed
    time.sleep(0.4)
    assert exits == [RESTART_EXIT_CODE]


def test_disarmed_does_not_fire(dog: StopWatchdog, exits: list[int]) -> None:
    """Disarming before the deadline prevents the exit."""
    dog.arm(time.monotonic() + 0.3)
    dog.disarm()
    time.sleep(0.7)
    assert exits == []


def test_rearmed_later_does_not_fire_early(dog: StopWatchdog, exits: list[int]) -> None:
    """Moving the deadline forward, as a renewal does, postpones the exit."""
    dog.arm(time.monotonic() + 0.3)
    dog.arm(time.monotonic() + 1.0)
    time.sleep(0.6)
    assert exits == []
    time.sleep(0.9)
    assert exits == [RESTART_EXIT_CODE]


def test_stop_ends_the_thread(exits: list[int]) -> None:
    """``stop`` disarms and joins the thread."""
    dog = StopWatchdog("test")
    dog.start()
    dog.arm(time.monotonic() + 0.2)
    dog.stop()
    assert not any(t.name == "test stop watchdog" for t in threading.enumerate())
    time.sleep(0.5)
    assert exits == []
    dog.stop()  # twice is fine


def test_can_start_again_after_stop(exits: list[int]) -> None:
    """A node that failed to start and retries reuses its watchdog."""
    dog = StopWatchdog("test")
    dog.start()
    dog.stop()
    dog.start()
    try:
        dog.arm(time.monotonic() + 0.2)
        time.sleep(0.6)
        assert exits == [RESTART_EXIT_CODE]
    finally:
        dog.stop()


def test_exits_in_time_although_a_hook_hangs_and_logging_is_blocked() -> None:
    """A hanging or failing hook and a blocked log handler cannot delay the exit."""
    exited: list[float] = []
    release = threading.Event()

    def _hang() -> None:
        release.wait(5)

    def _fail() -> None:
        raise RuntimeError("boom")

    blocked = logging.StreamHandler()
    logging.getLogger().addHandler(blocked)
    dog = StopWatchdog("test")
    dog.before_exit.extend([_fail, _hang])
    # The logging module's lock and a handler's lock are held by this thread,
    # as by a stalled event loop that logs to a slow disk.
    with (
        logging._lock,  # type: ignore[attr-defined]
        blocked.lock,  # type: ignore[union-attr]
        patch.object(watchdog.os, "_exit", lambda _: exited.append(time.monotonic())),
    ):
        dog.start()
        try:
            deadline = time.monotonic() + 0.2
            dog.arm(deadline)
            while not exited and time.monotonic() < deadline + 2:
                time.sleep(0.01)
        finally:
            release.set()
            dog.stop()
            logging.getLogger().removeHandler(blocked)
    assert exited
    assert exited[0] - deadline < 0.25
