"""A watchdog thread that ends the process when a deadline passes.

The event loop can stall or an integration can hang while it unloads. Neither
can be trusted to stop a node that lost its lease in time, so the deadline is
enforced from a thread that does not depend on the event loop.

Once the deadline passed, the thread does not log: a log handler that blocks
(on a slow disk) holds the lock of the logging module, and the thread would
wait for it. It writes one line to stderr instead. Clean-up hooks run in a
thread of their own and get ``BEFORE_EXIT_BUDGET``; the process ends then,
whether they returned or not.

Limits: the thread needs the GIL, so code that holds it (a C extension that
does not release it) can delay it past the deadline. It cannot take back
what an external system already accepted. A deployment that
needs a hard guarantee also needs a fence outside the process (a supervisor
that kills it, or a device that accepts commands of one node only).
"""

from collections.abc import Callable
from contextlib import suppress
import os
import threading
from time import monotonic
from typing import Final

from homeassistant.const import RESTART_EXIT_CODE

_POLL_INTERVAL: Final = 0.05
# How long the clean-up hooks may take before the process ends anyway.
BEFORE_EXIT_BUDGET: Final = 0.05


def _write_stderr(text: str) -> None:
    """Write to stderr without the logging module (and its locks)."""
    with suppress(OSError):
        os.write(2, text.encode(errors="replace"))


def _run_hooks(hooks: list[Callable[[], None]]) -> None:
    for cb in hooks:
        try:
            cb()
        except Exception as err:  # noqa: BLE001
            _write_stderr(f"Watchdog clean-up failed: {err!r}\n")


class StopWatchdog:
    """End the process with the restart exit code once a deadline passed."""

    def __init__(self, name: str = "cluster") -> None:
        """Initialize the watchdog; the thread starts with ``start``."""
        self._name = name
        self._lock = threading.Lock()
        self._deadline: float | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        # The exit code of the process when the deadline passes.
        self.exit_code = RESTART_EXIT_CODE
        # Called from a thread of their own just before the process ends,
        # for work that outlives the process otherwise (Supervisor add-ons).
        # They must not block: the process ends after BEFORE_EXIT_BUDGET.
        self.before_exit: list[Callable[[], None]] = []

    @property
    def armed(self) -> bool:
        """Return True if a deadline is set."""
        with self._lock:
            return self._deadline is not None

    def start(self) -> None:
        """Start the daemon thread."""
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name=f"{self._name} stop watchdog", daemon=True
        )
        self._thread.start()

    def arm(self, deadline: float) -> None:
        """End the process when ``time.monotonic()`` reaches ``deadline``."""
        with self._lock:
            self._deadline = deadline

    def disarm(self) -> None:
        """Forget the deadline."""
        with self._lock:
            self._deadline = None

    def stop(self) -> None:
        """Disarm, then end and join the thread."""
        self.disarm()
        self._stop.set()
        if (thread := self._thread) is not None:
            self._thread = None
            thread.join()

    def _run(self) -> None:
        while not self._stop.wait(_POLL_INTERVAL):
            with self._lock:
                deadline = self._deadline
                if deadline is None or monotonic() < deadline:
                    continue
                # Fire once.
                self._deadline = None
            _write_stderr(
                f"{self._name} could not stop in time after losing leadership;"
                " ending the process to prevent split brain\n"
            )
            if hooks := list(self.before_exit):
                runner = threading.Thread(
                    target=_run_hooks,
                    args=(hooks,),
                    name=f"{self._name} watchdog clean-up",
                    daemon=True,
                )
                runner.start()
                runner.join(BEFORE_EXIT_BUDGET)
            # Looked up at call time on purpose: tests replace it.
            os._exit(self.exit_code)
