"""Idempotent webhook processing with retries, a dead-letter list and reconciliation.

Webhooks are delivered at least once, not exactly once. A retry after a timeout
must not create a second lead, invoice or message. The key idea: store the
idempotency key in a database with a UNIQUE constraint BEFORE doing the work.

Standard library only (sqlite3).
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from typing import Any, Callable, Dict, Iterable, List, Set

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    key        TEXT PRIMARY KEY,
    source     TEXT NOT NULL,
    external_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload    TEXT NOT NULL,
    status     TEXT NOT NULL,          -- processing | done | failed | dead
    attempts   INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    updated_at REAL NOT NULL
);
"""


def idempotency_key(source: str, external_id: str, event_type: str) -> str:
    """Derive the key from the event identity, never from the time of arrival."""
    raw = f"{source}\x1f{external_id}\x1f{event_type}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


class IdempotentProcessor:
    def __init__(self, path: str = ":memory:", max_attempts: int = 3) -> None:
        self.db = sqlite3.connect(path)
        self.db.executescript(SCHEMA)
        self.max_attempts = max_attempts

    def process(self, source: str, external_id: str, event_type: str,
                payload: Dict[str, Any], handler: Callable[[Dict[str, Any]], None]) -> str:
        """Returns 'done', 'duplicate' or 'failed'."""
        key = idempotency_key(source, external_id, event_type)
        cur = self.db.execute(
            "INSERT OR IGNORE INTO events(key, source, external_id, event_type, payload, status, attempts, updated_at)"
            " VALUES (?, ?, ?, ?, ?, 'processing', 0, ?)",
            (key, source, external_id, event_type, json.dumps(payload, sort_keys=True), time.time()),
        )
        self.db.commit()
        if cur.rowcount == 0:
            return "duplicate"
        return self._run(key, payload, handler)

    def retry_failed(self, handler: Callable[[Dict[str, Any]], None]) -> Dict[str, int]:
        """Retry every event in 'failed' status once.

        Call this on a schedule (add your own backoff by spacing the calls).
        Events that reach max_attempts move to 'dead' so that someone gets alerted
        instead of the lead silently vanishing.
        """
        stats = {"done": 0, "failed": 0, "dead": 0}
        rows = self.db.execute("SELECT key, payload FROM events WHERE status = 'failed'").fetchall()
        for key, payload_json in rows:
            outcome = self._run(key, json.loads(payload_json), handler)
            stats[outcome] += 1
        return stats

    def dead_letters(self) -> List[Dict[str, Any]]:
        rows = self.db.execute(
            "SELECT source, external_id, event_type, attempts, last_error FROM events WHERE status = 'dead'"
        ).fetchall()
        return [dict(zip(("source", "external_id", "event_type", "attempts", "last_error"), r)) for r in rows]

    def reconcile(self, source: str, event_type: str, ids_in_source: Iterable[str]) -> Set[str]:
        """Return ids that the source says exist but we never processed to 'done'.

        Run this nightly. It finds the records that vanished without an error.
        """
        done = {
            row[0]
            for row in self.db.execute(
                "SELECT external_id FROM events WHERE source = ? AND event_type = ? AND status = 'done'",
                (source, event_type),
            )
        }
        return set(ids_in_source) - done

    def _run(self, key: str, payload: Dict[str, Any], handler: Callable[[Dict[str, Any]], None]) -> str:
        try:
            handler(payload)
        except Exception as exc:  # noqa: BLE001 - we record every failure reason
            attempts = self.db.execute("SELECT attempts FROM events WHERE key = ?", (key,)).fetchone()[0] + 1
            status = "dead" if attempts >= self.max_attempts else "failed"
            self.db.execute(
                "UPDATE events SET status = ?, attempts = ?, last_error = ?, updated_at = ? WHERE key = ?",
                (status, attempts, repr(exc), time.time(), key),
            )
            self.db.commit()
            return status
        self.db.execute("UPDATE events SET status = 'done', updated_at = ? WHERE key = ?", (time.time(), key))
        self.db.commit()
        return "done"
