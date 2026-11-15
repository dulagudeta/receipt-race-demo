"""
Tests for receipt_manager.

The race-reproduction test uses threading.Barrier to force the exact
interleaving where both threads pass the existence check before either
appends to the store. No sleep() or timing tricks are used.
"""

from __future__ import annotations

import threading
import pytest
import receipt_manager


@pytest.fixture(autouse=True)
def fresh_store():
    """Clear the shared store before every test."""
    receipt_manager.reset_store()


# ---------------------------------------------------------------------------
# Race condition reproduction
# ---------------------------------------------------------------------------

def test_duplicate_terminal_receipt_race() -> None:
    """
    Force the check-then-act race deterministically.

    record_terminal() has two steps:
        1. CHECK: get_receipt(execution_id)  → None (no receipt yet)
        2. ACT:   _store.append(...)

    A threading.Barrier(2) is injected between steps 1 and 2 by patching
    get_receipt. Both threads complete the check, then wait at the barrier
    until both have arrived. Only then are both released to append. This
    guarantees the duplicate-write path is taken every time, without any
    sleep() or probabilistic timing.

    Expected behaviour (correct system): exactly one receipt.
    Actual behaviour (buggy system):     two receipts — test FAILS.
    """
    execution_id = "exec-001"
    errors: list[Exception] = []

    barrier = threading.Barrier(2)
    original_get_receipt = receipt_manager.get_receipt

    def get_receipt_with_barrier(eid: str) -> str | None:
        """
        Wraps get_receipt: after returning None (empty store), both threads
        pause at the barrier, ensuring neither has written yet when both
        proceed to the append step.
        """
        result = original_get_receipt(eid)
        if result is None:
            # Both threads park here until both have confirmed the store
            # is empty. This is the moment the race window is widest.
            barrier.wait(timeout=5)
        return result

    def run(state: str) -> None:
        try:
            receipt_manager.record_terminal(execution_id, state)
        except Exception as exc:
            errors.append(exc)

    receipt_manager.get_receipt = get_receipt_with_barrier  # type: ignore[assignment]
    try:
        t1 = threading.Thread(target=run, args=("done",), name="stage")
        t2 = threading.Thread(target=run, args=("timeout",), name="watchdog")
        t1.start()
        t2.start()
        t1.join(timeout=10)
        t2.join(timeout=10)
    finally:
        receipt_manager.get_receipt = original_get_receipt  # type: ignore[assignment]

    assert not errors, f"Unexpected errors in threads: {errors}"
    assert not t1.is_alive() and not t2.is_alive(), "Threads timed out"

    # Count how many receipts exist for the execution.
    all_receipts = [(eid, s) for eid, s in receipt_manager._store
                    if eid == execution_id]

    # Exactly one terminal receipt must exist.
    # The buggy implementation produces two entries → assertion FAILS.
    assert len(all_receipts) == 1, (
        f"Race condition triggered: expected 1 receipt, found "
        f"{len(all_receipts)}: {[s for _, s in all_receipts]}"
    )
