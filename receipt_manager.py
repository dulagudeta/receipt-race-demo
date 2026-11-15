"""
Receipt manager with an intentional check-then-act race condition.

A receipt records the terminal state of a processing execution:
either 'done' (stage completion) or 'timeout' (watchdog timeout).
Only one terminal receipt should ever exist per execution_id.

BUG: record_terminal() reads the current state and writes in separate
steps without holding any lock. Two threads can both observe no existing
receipt and both proceed to insert, producing duplicate terminal records.
"""

import sqlite3
import threading
from typing import Optional


_DB_PATH = ":memory:"
_connection: Optional[sqlite3.Connection] = None
_connection_lock = threading.Lock()


def _get_connection() -> sqlite3.Connection:
    """Return the shared in-memory SQLite connection (created once)."""
    global _connection
    with _connection_lock:
        if _connection is None:
            _connection = sqlite3.connect(_DB_PATH, check_same_thread=False)
            _connection.execute(
                """
                CREATE TABLE IF NOT EXISTS receipts (
                    execution_id TEXT NOT NULL,
                    state        TEXT NOT NULL
                )
                """
            )
            _connection.commit()
        return _connection


def reset_db() -> None:
    """Drop and recreate the receipts table. Use only in tests."""
    global _connection
    with _connection_lock:
        if _connection is not None:
            _connection.execute("DROP TABLE IF EXISTS receipts")
            _connection.execute(
                """
                CREATE TABLE receipts (
                    execution_id TEXT NOT NULL,
                    state        TEXT NOT NULL
                )
                """
            )
            _connection.commit()
        else:
            _get_connection()


def get_receipt(execution_id: str) -> Optional[str]:
    """Return the recorded terminal state, or None if no receipt exists."""
    conn = _get_connection()
    row = conn.execute(
        "SELECT state FROM receipts WHERE execution_id = ?",
        (execution_id,),
    ).fetchone()
    return row[0] if row else None


def record_terminal(execution_id: str, state: str) -> bool:
    """
    Attempt to record a terminal state for execution_id.

    Returns True if the receipt was written, False if one already existed.

    RACE CONDITION: the check and the write are not atomic. Two threads
    can both pass the check before either completes the write.
    """
    conn = _get_connection()

    # CHECK — reads current state
    existing = get_receipt(execution_id)
    if existing is not None:
        return False

    # ACT — writes new state (gap between check and write is exploitable)
    conn.execute(
        "INSERT INTO receipts (execution_id, state) VALUES (?, ?)",
        (execution_id, state),
    )
    conn.commit()
    return True
