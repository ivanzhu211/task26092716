"""Audit statistics for a document-extraction pipeline, backed by SQLite.

Raw events are kept for 365 days and then physically deleted by ``prune``;
all statistics remain queryable forever because they are maintained as
incremental aggregates at append time.  The only per-event state retained
indefinitely is the minimal correction-attribution state for documents
that are not yet finalized (approved).

Concurrency: the database runs in WAL mode, so queries never block behind
``prune`` or concurrent ``append`` calls.  Every ``append`` is a single
short write transaction; ``prune`` deletes at most PRUNE_BATCH raw events
per transaction and commits between batches, so it is kill-safe and can be
re-run to convergence at any point.
"""

from __future__ import annotations

import sqlite3
import time
from dataclasses import dataclass
from typing import Optional

SECONDS_PER_DAY = 86400
RETENTION_SECONDS = 365 * SECONDS_PER_DAY
LATE_WINDOW_SECONDS = 72 * 3600  # events may arrive up to 72h "late"
PRUNE_BATCH = 1000               # max raw events deleted per write txn

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    k TEXT PRIMARY KEY,
    v INTEGER NOT NULL
);
-- Raw events; pruned after 365 days.  Never read by queries.
CREATE TABLE IF NOT EXISTS events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    ts INTEGER NOT NULL,
    kind TEXT NOT NULL,
    doc_id TEXT NOT NULL,
    doc_type TEXT,
    field TEXT,
    confidence REAL
);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);
-- Permanent per-day aggregates (one row per day / day+type: O(1) per day).
CREATE TABLE IF NOT EXISTS daily_volume (
    day INTEGER PRIMARY KEY,
    cnt INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS hist (
    day INTEGER NOT NULL,
    doc_type TEXT NOT NULL,
    bucket INTEGER NOT NULL,
    cnt INTEGER NOT NULL,
    PRIMARY KEY (day, doc_type, bucket)
);
CREATE TABLE IF NOT EXISTS accuracy (
    day INTEGER PRIMARY KEY,
    total INTEGER NOT NULL,
    corrected INTEGER NOT NULL
);
-- Distinct-doc dedup sets, only for days still inside the late-arrival
-- window; rolled up (deleted, count already in daily_volume) afterwards.
CREATE TABLE IF NOT EXISTS daily_docs (
    day INTEGER NOT NULL,
    doc_id TEXT NOT NULL,
    PRIMARY KEY (day, doc_id)
);
-- Minimal correction-attribution state for not-yet-finalized documents.
CREATE TABLE IF NOT EXISTS pend_ext (
    doc_id TEXT NOT NULL,
    field TEXT NOT NULL,
    ts INTEGER NOT NULL,
    day INTEGER NOT NULL,
    corr INTEGER NOT NULL,          -- # corrections currently attributed
    PRIMARY KEY (doc_id, field, ts)
);
CREATE INDEX IF NOT EXISTS idx_pend_ext_ts ON pend_ext(ts);
CREATE TABLE IF NOT EXISTS pend_corr (
    doc_id TEXT NOT NULL,
    field TEXT NOT NULL,
    ts INTEGER NOT NULL,
    target INTEGER,                 -- ts of the extraction it maps to
    PRIMARY KEY (doc_id, field, ts)
);
CREATE TABLE IF NOT EXISTS docs (
    doc_id TEXT PRIMARY KEY,
    approved_ts INTEGER NOT NULL
);
"""


@dataclass(frozen=True)
class Event:
    kind: str            # 'extracted' | 'corrected' | 'approved'
    ts: int              # epoch seconds (UTC)
    doc_id: str
    doc_type: Optional[str] = None
    field: Optional[str] = None
    confidence: Optional[float] = None


def extracted(ts: int, doc_id: str, doc_type: str, field: str, confidence: float) -> Event:
    return Event("extracted", ts, doc_id, doc_type, field, confidence)


def corrected(ts: int, doc_id: str, field: str) -> Event:
    return Event("corrected", ts, doc_id, None, field, None)


def approved(ts: int, doc_id: str) -> Event:
    return Event("approved", ts, doc_id)


def day_of(ts: int) -> int:
    return ts // SECONDS_PER_DAY


def _bucket(confidence: float) -> int:
    b = int(confidence * 10)
    return 9 if b > 9 else (0 if b < 0 else b)


class AuditStore:
    def __init__(self, path: str):
        self._con = sqlite3.connect(path, timeout=60, isolation_level=None)
        self._con.execute("PRAGMA journal_mode=WAL")
        self._con.execute("PRAGMA busy_timeout=60000")
        self._con.execute("PRAGMA synchronous=NORMAL")
        self._con.executescript(_SCHEMA)
        self._con.execute(
            "INSERT OR IGNORE INTO meta(k, v) VALUES ('H', 0), ('appended', 0)"
        )

    # ------------------------------------------------------------------
    # append
    # ------------------------------------------------------------------
    def append(self, event: Event) -> None:
        con = self._con
        con.execute("BEGIN IMMEDIATE")
        try:
            horizon = self._bump_horizon(event.ts)
            con.execute(
                "INSERT INTO events(ts, kind, doc_id, doc_type, field, confidence)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (event.ts, event.kind, event.doc_id, event.doc_type,
                 event.field, event.confidence),
            )
            if event.kind == "extracted":
                self._on_extracted(event, horizon)
            elif event.kind == "corrected":
                self._on_corrected(event)
            else:
                self._on_approved(event)
            self._housekeep(horizon)
            con.execute("UPDATE meta SET v = v + 1 WHERE k = 'appended'")
            con.execute("COMMIT")
        except BaseException:
            con.execute("ROLLBACK")
            raise

    def _bump_horizon(self, ts: int) -> int:
        con = self._con
        row = con.execute("SELECT v FROM meta WHERE k = 'H'").fetchone()
        horizon = max(row[0], ts)
        if horizon != row[0]:
            con.execute("UPDATE meta SET v = ? WHERE k = 'H'", (horizon,))
        return horizon

    def _on_extracted(self, ev: Event, horizon: int) -> None:
        con = self._con
        day = day_of(ev.ts)
        cur = con.execute(
            "INSERT OR IGNORE INTO daily_docs(day, doc_id) VALUES (?, ?)",
            (day, ev.doc_id),
        )
        if cur.rowcount:
            con.execute(
                "INSERT INTO daily_volume(day, cnt) VALUES (?, 1)"
                " ON CONFLICT(day) DO UPDATE SET cnt = cnt + 1",
                (day,),
            )
        con.execute(
            "INSERT INTO hist(day, doc_type, bucket, cnt) VALUES (?, ?, ?, 1)"
            " ON CONFLICT(day, doc_type, bucket) DO UPDATE SET cnt = cnt + 1",
            (day, ev.doc_type, _bucket(ev.confidence)),
        )
        con.execute(
            "INSERT INTO accuracy(day, total, corrected) VALUES (?, 1, 0)"
            " ON CONFLICT(day) DO UPDATE SET total = total + 1",
            (day,),
        )
        con.execute(
            "INSERT INTO pend_ext(doc_id, field, ts, day, corr) VALUES (?, ?, ?, ?, 0)",
            (ev.doc_id, ev.field, ev.ts, day),
        )
        # Corrections that arrived before this extraction but belong to it
        # (they currently target the previous extraction, or nothing).
        succ = con.execute(
            "SELECT MIN(ts) FROM pend_ext WHERE doc_id = ? AND field = ? AND ts > ?",
            (ev.doc_id, ev.field, ev.ts),
        ).fetchone()[0]
        if succ is None:
            rows = con.execute(
                "SELECT ts, target FROM pend_corr"
                " WHERE doc_id = ? AND field = ? AND ts > ?",
                (ev.doc_id, ev.field, ev.ts),
            ).fetchall()
        else:
            rows = con.execute(
                "SELECT ts, target FROM pend_corr"
                " WHERE doc_id = ? AND field = ? AND ts > ? AND ts < ?",
                (ev.doc_id, ev.field, ev.ts, succ),
            ).fetchall()
        gained = 0
        for corr_ts, target in rows:
            if target is not None:
                self._decrement_corr(ev.doc_id, ev.field, target)
            con.execute(
                "UPDATE pend_corr SET target = ?"
                " WHERE doc_id = ? AND field = ? AND ts = ?",
                (ev.ts, ev.doc_id, ev.field, corr_ts),
            )
            gained += 1
        if gained:
            con.execute(
                "UPDATE pend_ext SET corr = ? WHERE doc_id = ? AND field = ? AND ts = ?",
                (gained, ev.doc_id, ev.field, ev.ts),
            )
            con.execute(
                "UPDATE accuracy SET corrected = corrected + 1 WHERE day = ?",
                (day,),
            )
        self._cleanup_field(ev.doc_id, ev.field, horizon)

    def _on_corrected(self, ev: Event) -> None:
        con = self._con
        row = con.execute(
            "SELECT ts, day, corr FROM pend_ext"
            " WHERE doc_id = ? AND field = ? AND ts < ?"
            " ORDER BY ts DESC LIMIT 1",
            (ev.doc_id, ev.field, ev.ts),
        ).fetchone()
        target = row[0] if row else None
        con.execute(
            "INSERT INTO pend_corr(doc_id, field, ts, target) VALUES (?, ?, ?, ?)",
            (ev.doc_id, ev.field, ev.ts, target),
        )
        if row is not None:
            target_ts, target_day, corr = row
            con.execute(
                "UPDATE pend_ext SET corr = corr + 1"
                " WHERE doc_id = ? AND field = ? AND ts = ?",
                (ev.doc_id, ev.field, target_ts),
            )
            if corr == 0:
                con.execute(
                    "UPDATE accuracy SET corrected = corrected + 1 WHERE day = ?",
                    (target_day,),
                )

    def _on_approved(self, ev: Event) -> None:
        self._con.execute(
            "INSERT INTO docs(doc_id, approved_ts) VALUES (?, ?)"
            " ON CONFLICT(doc_id) DO UPDATE"
            " SET approved_ts = MAX(approved_ts, excluded.approved_ts)",
            (ev.doc_id, ev.ts),
        )

    def _decrement_corr(self, doc_id: str, field: str, ext_ts: int) -> None:
        con = self._con
        con.execute(
            "UPDATE pend_ext SET corr = corr - 1"
            " WHERE doc_id = ? AND field = ? AND ts = ?",
            (doc_id, field, ext_ts),
        )
        row = con.execute(
            "SELECT day, corr FROM pend_ext WHERE doc_id = ? AND field = ? AND ts = ?",
            (doc_id, field, ext_ts),
        ).fetchone()
        if row is not None and row[1] == 0:
            con.execute(
                "UPDATE accuracy SET corrected = corrected - 1 WHERE day = ?",
                (row[0],),
            )

    # ------------------------------------------------------------------
    # cleanup of derived state (aggregates are never touched by this)
    # ------------------------------------------------------------------
    def _cleanup_field(self, doc_id: str, field: str, horizon: int) -> int:
        """Drop attribution state that no future event can reference.

        A correction not yet seen would have ts >= horizon - 72h, so:
        - corrections with ts < cutoff can never be re-attributed;
        - extractions older than the newest sub-cutoff extraction can
          never become a correction's target.
        Returns the number of rows deleted.
        """
        con = self._con
        cutoff = horizon - LATE_WINDOW_SECONDS
        boundary = con.execute(
            "SELECT MAX(ts) FROM pend_ext WHERE doc_id = ? AND field = ? AND ts < ?",
            (doc_id, field, cutoff),
        ).fetchone()[0]
        deleted = 0
        if boundary is not None:
            deleted += con.execute(
                "DELETE FROM pend_ext WHERE doc_id = ? AND field = ? AND ts < ?",
                (doc_id, field, boundary),
            ).rowcount
        deleted += con.execute(
            "DELETE FROM pend_corr WHERE doc_id = ? AND field = ? AND ts < ?",
            (doc_id, field, cutoff),
        ).rowcount
        return deleted

    def _housekeep(self, horizon: int) -> None:
        """Small bounded cleanup done inline on each append."""
        con = self._con
        cutoff = horizon - LATE_WINDOW_SECONDS
        # Finalize documents whose approval is older than the late window:
        # no further events for them can ever arrive.
        for (doc_id,) in con.execute(
            "SELECT doc_id FROM docs WHERE approved_ts < ? LIMIT 4",
            (cutoff,),
        ).fetchall():
            con.execute("DELETE FROM pend_ext WHERE doc_id = ?", (doc_id,))
            con.execute("DELETE FROM pend_corr WHERE doc_id = ?", (doc_id,))
            con.execute("DELETE FROM docs WHERE doc_id = ?", (doc_id,))
        # Roll up distinct-doc dedup sets for fully expired days.
        con.execute(
            "DELETE FROM daily_docs WHERE rowid IN"
            " (SELECT rowid FROM daily_docs WHERE (day + 1) * ? <= ? LIMIT 200)",
            (SECONDS_PER_DAY, cutoff),
        )

    # ------------------------------------------------------------------
    # queries
    # ------------------------------------------------------------------
    def daily_volume(self, day: int) -> int:
        row = self._con.execute(
            "SELECT cnt FROM daily_volume WHERE day = ?", (day,)
        ).fetchone()
        return row[0] if row else 0

    def confidence_histogram(self, day: int, doc_type: str) -> list[int]:
        result = [0] * 10
        for bucket, cnt in self._con.execute(
            "SELECT bucket, cnt FROM hist WHERE day = ? AND doc_type = ?",
            (day, doc_type),
        ):
            result[bucket] = cnt
        return result

    def accuracy(self, day: int) -> tuple[int, int]:
        row = self._con.execute(
            "SELECT total, corrected FROM accuracy WHERE day = ?", (day,)
        ).fetchone()
        if row is None:
            return (0, 0)
        return (row[0], row[0] - row[1])

    def snapshot(self, day: int, doc_type: str) -> tuple[int, int, list[int], tuple[int, int]]:
        """Atomic read: (appended_count, volume, histogram, accuracy).

        The appended count and the statistics come from a single read
        snapshot, so they describe exactly the same set of events.
        """
        con = self._con
        con.execute("BEGIN")
        try:
            appended = con.execute(
                "SELECT v FROM meta WHERE k = 'appended'"
            ).fetchone()[0]
            result = (
                appended,
                self.daily_volume(day),
                self.confidence_histogram(day, doc_type),
                self.accuracy(day),
            )
            con.execute("COMMIT")
            return result
        except BaseException:
            con.execute("ROLLBACK")
            raise

    # ------------------------------------------------------------------
    # prune
    # ------------------------------------------------------------------
    def prune(self, now: int, batch_sleep: float = 0.01) -> None:
        """Delete raw events older than now - 365d, in kill-safe batches.

        Each write transaction removes at most PRUNE_BATCH raw events and
        commits, so other processes can append/query between batches and a
        crash simply means the next run resumes where this one stopped.
        Also advances the horizon H, which retires derived state that has
        fallen out of the late-arrival window.
        """
        cutoff = now - RETENTION_SECONDS
        while True:
            self._con.execute("BEGIN IMMEDIATE")
            try:
                self._bump_horizon(now)
                deleted = self._con.execute(
                    "DELETE FROM events WHERE seq IN"
                    " (SELECT seq FROM events WHERE ts < ? ORDER BY seq LIMIT ?)",
                    (cutoff, PRUNE_BATCH),
                ).rowcount
                cleaned = self._cleanup_round()
                self._con.execute("COMMIT")
            except BaseException:
                self._con.execute("ROLLBACK")
                raise
            if deleted == 0 and cleaned == 0:
                break
            time.sleep(batch_sleep)  # yield the write lock between batches

    def _cleanup_round(self) -> int:
        """One bounded round of derived-state cleanup; returns rows touched."""
        con = self._con
        horizon = con.execute("SELECT v FROM meta WHERE k = 'H'").fetchone()[0]
        cutoff = horizon - LATE_WINDOW_SECONDS
        done = 0
        # Finalize documents whose approval fell out of the late window.
        for (doc_id,) in con.execute(
            "SELECT doc_id FROM docs WHERE approved_ts < ? LIMIT 50",
            (cutoff,),
        ).fetchall():
            done += con.execute(
                "DELETE FROM pend_ext WHERE doc_id = ?", (doc_id,)
            ).rowcount
            done += con.execute(
                "DELETE FROM pend_corr WHERE doc_id = ?", (doc_id,)
            ).rowcount
            con.execute("DELETE FROM docs WHERE doc_id = ?", (doc_id,))
        # Retire attribution state older than the late window, a few
        # doc+field pairs at a time.  Only pairs that still have
        # deletable rows are selected, so every round makes progress and
        # the loop converges.
        pairs = con.execute(
            "SELECT doc_id, field FROM ("
            "  SELECT doc_id, field FROM pend_ext WHERE ts < ?"
            "  GROUP BY doc_id, field HAVING COUNT(*) >= 2"
            "  UNION"
            "  SELECT doc_id, field FROM pend_corr WHERE ts < ?"
            ") LIMIT 50",
            (cutoff, cutoff),
        ).fetchall()
        for doc_id, field in pairs:
            done += self._cleanup_field(doc_id, field, horizon)
        # Roll up expired distinct-doc dedup sets.
        done += con.execute(
            "DELETE FROM daily_docs WHERE rowid IN"
            " (SELECT rowid FROM daily_docs WHERE (day + 1) * ? <= ? LIMIT 1000)",
            (SECONDS_PER_DAY, cutoff),
        ).rowcount
        return done

    def close(self) -> None:
        self._con.close()
