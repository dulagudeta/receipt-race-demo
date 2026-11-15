"""
Receipt manager — fixed implementation.

The check-and-write in record_terminal() is now protected by _receipt_lock,
making the state transition atomic from the perspective of competing threads
in the same process.

Thread-level guarantee: _receipt_lock prevents concurrent access from any
thread that holds a reference to this module.

Process-level guarantee: not provided by this lock. If multiple processes
(or distributed workers) call record_terminal() concurrently, a database
uniqueness constraint (UNIQUE index + INSERT OR IGNORE, or SELECT FOR UPDATE
in a transactional DB) is required. See README for the full discussion.
"""

from __future__ import annotations

import threading
from typing import Optional

# Backing store: list of (execution_id, state) tuples.
# A list is used so duplicate receipts accumulate as separate entries —
# making violations visible in tests.
_store: list[tuple[str, str]] = []

# Shared lock. Both record_terminal() paths (stage completion and watchdog)
# must acquire this lock before checking or writing. This makes the combined
# check-and-write atomic from the perspective of all threads in this process.
_receipt_lock = threading.Lock()


def reset_store() -> None:
    """Clear all receipts. Use only in tests."""
    _store.clear()


def get_receipt(execution_id: str) -> Optional[str]:
    """Return the recorded terminal state, or None if no receipt exists."""
    for eid, state in _store:
        if eid == execution_id:
            return state
    return None


def record_terminal(execution_id: str, state: str) -> bool:
    """
    Attempt to record a terminal state for execution_id.

    Returns True if the receipt was written, False if one already existed.

    The lock ensures that only one thread can execute the check-and-write
    sequence at a time. Any thread that loses the race sees the existing
    receipt during the check and returns False without writing.
    """
    with _receipt_lock:
        if get_receipt(execution_id) is not None:
            return False
        _store.append((execution_id, state))
        return True
