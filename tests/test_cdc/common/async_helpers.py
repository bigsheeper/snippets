"""Threading helpers for concurrent test operations."""
import threading
from typing import Callable


def start_async(fn: Callable, *args, **kwargs):
    """Run fn(*args, **kwargs) in a daemon thread.

    Returns (thread, join_fn). join_fn(timeout) raises if the thread didn't finish
    or if fn raised an exception.
    """
    result = {"exc": None}

    def _run():
        try:
            fn(*args, **kwargs)
        except Exception as e:
            result["exc"] = e

    t = threading.Thread(target=_run, daemon=True)
    t.start()

    def join(timeout=None):
        t.join(timeout)
        if t.is_alive():
            raise TimeoutError("async operation did not finish in time")
        if result["exc"]:
            raise result["exc"]

    return t, join
