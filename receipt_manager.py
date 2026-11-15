"""
Receipt manager with an intentional check-then-act race condition.

A receipt records the terminal state of a processing execution:
either 'done' (stage completion) or 'timeout' (watchdog timeout).
Only one terminal receipt should ever exist per execution_id.

The store is an unsynchronized list of (execution_id, state) tuples.
Using a list (rather than a dict) makes duplicate receipts visible as
distinct entries — which is what we need to prove the race occurred.

BUG: record_terminal() reads the current state and writes in separate
steps without holding any lock. Two threads can both observe no existing
receipt and both proceed to append, producing duplicate terminal records.
"""

from __future__ import annotations

import threading
from typing import Optional

# Backing store: list of (execution_id, state) tuples.
# A list is used so duplicate receipts accumulate as separate entries.
# No internal synchronization — the race is real.
_store: list[tuple[str, str]] = []

# Lock used in the FIXED version. Defined here so the module structure
# is identical before and after the fix.
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

    RACE CONDITION: the check and the write are not atomic. Two threads
    can both pass the existence check before either appends to the store,
    resulting in two receipts for the same execution_id.
    """
    # CHECK — reads current state (not protected by any lock)
    if get_receipt(execution_id) is not None:
        return False

    # ACT — writes new state
    # Gap between check and write above is exploitable by a racing thread.
    _store.append((execution_id, state))
    return True
