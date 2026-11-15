"""
Tests for receipt_manager.

Test plan:
  - test_duplicate_terminal_receipt_race: deterministic concurrency test
    that MUST FAIL against the buggy implementation and PASS after the fix.
  - test_stage_completion_wins: stage records 'done'; watchdog cannot overwrite.
  - test_watchdog_wins: watchdog records 'timeout'; stage cannot overwrite.
  - test_no_duplicate_after_terminal: repeated calls after terminal state return False.
  - test_independent_executions: receipts for different IDs are independent.
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
    After the fix, _receipt_lock serializes check+write → test PASSES.
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

        After the fix, _receipt_lock is acquired before get_receipt is called,
        so only one thread enters at a time — the second thread blocks on the
        lock and never reaches this barrier path with an empty store. The
        barrier fires immediately for the one thread that sees None, and
        correctly times out for the second thread that sees the existing
        receipt (which returns early). To avoid a barrier deadlock after the
        fix, we use barrier.abort() on timeout.
        """
        result = original_get_receipt(eid)
        if result is None:
            try:
                barrier.wait(timeout=2)
            except threading.BrokenBarrierError:
                pass  # Fixed implementation: second thread never arrives here
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

    all_receipts = [(eid, s) for eid, s in receipt_manager._store
                    if eid == execution_id]

    # Exactly one terminal receipt must exist.
    assert len(all_receipts) == 1, (
        f"Race condition triggered: expected 1 receipt, found "
        f"{len(all_receipts)}: {[s for _, s in all_receipts]}"
    )


# ---------------------------------------------------------------------------
# Functional correctness tests
# ---------------------------------------------------------------------------

def test_stage_completion_wins() -> None:
    """Stage records 'done' first; subsequent watchdog call returns False."""
    written = receipt_manager.record_terminal("exec-002", "done")
    assert written is True
    assert receipt_manager.get_receipt("exec-002") == "done"

    written_again = receipt_manager.record_terminal("exec-002", "timeout")
    assert written_again is False
    assert receipt_manager.get_receipt("exec-002") == "done"


def test_watchdog_wins() -> None:
    """Watchdog records 'timeout' first; subsequent stage call returns False."""
    written = receipt_manager.record_terminal("exec-003", "timeout")
    assert written is True
    assert receipt_manager.get_receipt("exec-003") == "timeout"

    written_again = receipt_manager.record_terminal("exec-003", "done")
    assert written_again is False
    assert receipt_manager.get_receipt("exec-003") == "timeout"


def test_no_duplicate_after_terminal() -> None:
    """Repeated record_terminal calls after a terminal state all return False."""
    receipt_manager.record_terminal("exec-004", "done")

    for _ in range(5):
        result = receipt_manager.record_terminal("exec-004", "done")
        assert result is False
        result = receipt_manager.record_terminal("exec-004", "timeout")
        assert result is False

    receipts = [(eid, s) for eid, s in receipt_manager._store
                if eid == "exec-004"]
    assert len(receipts) == 1


def test_independent_executions() -> None:
    """Receipts for different execution IDs are stored independently."""
    receipt_manager.record_terminal("exec-A", "done")
    receipt_manager.record_terminal("exec-B", "timeout")

    assert receipt_manager.get_receipt("exec-A") == "done"
    assert receipt_manager.get_receipt("exec-B") == "timeout"
    assert receipt_manager.get_receipt("exec-C") is None


def test_no_receipt_before_recording() -> None:
    """get_receipt returns None for an unknown execution_id."""
    assert receipt_manager.get_receipt("never-seen") is None
