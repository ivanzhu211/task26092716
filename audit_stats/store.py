"""SQLite-backed audit statistics store.

Raw events are kept for 365 days and then physically deleted by ``prune``.
Query results are served from incrementally maintained aggregate tables, so
they are unaffected by pruning.  Correction attribution for not-yet-approved
documents is kept in small per-(doc, field) "hot state" tables holding only
events inside the 72h late-arrival window plus the single latest extraction
below that window.
"""
from __future__ import annotations

import sqlite3
from bisect import bisect_left
from typing import List, Tuple

from .events import KIND_APPROVED, KIND_CORRECTED, KIND_EXTRACTED, Event

DAY_SECONDS = 86400
LATE_WINDOW_SECONDS = 72 * 3600
RETENTION_SECONDS = 365 * DAY_SECONDS
PRUNE_BATCH = 1000
STATE_COLLAPSE_BATCH = 50

_SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS meta (
    k TEXT PRIMARY KEY,
    v REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,
    ts REAL NOT NULL,
    doc_id TEXT NOT NULL,
    doc_type TEXT,
    field TEXT,
    confidence REAL
);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts);
CREATE TABLE IF NOT EXISTS doc_day (
    doc_id TEXT NOT NULL,
    day INTEGER NOT NULL,
    PRIMARY KEY (doc_id, day)
);
CREATE TABLE IF NOT EXISTS agg_vol (
    day INTEGER PRIMARY KEY,
    n INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS agg_hist (
    day INTEGER NOT NULL,
    doc_type TEXT NOT NULL,
    bucket INTEGER NOT NULL,
    n INTEGER NOT NULL,
    PRIMARY KEY (day, doc_type, bucket)
);
CREATE TABLE IF NOT EXISTS agg_acc (
    day INTEGER PRIMARY KEY,
    total INTEGER NOT NULL,
    uncorrected INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS st_extr (
    doc_id TEXT NOT NULL,
    field TEXT NOT NULL,
    ets REAL NOT NULL,
    day INTEGER NOT NULL,
    perm INTEGER NOT NULL DEFAULT 0,
    tmp INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (doc_id, field, ets)
);
CREATE INDEX IF NOT EXISTS idx_st_extr_ets ON st_extr(ets);
CREATE TABLE IF NOT EXISTS st_corr (
    doc_id TEXT NOT NULL,
    field TEXT NOT NULL,
    cts REAL NOT NULL,
    PRIMARY KEY (doc_id, field, cts)
);
CREATE INDEX IF NOT EXISTS idx_st_corr_cts ON st_corr(cts);
"""


def _bucket(confidence: float) -> int:
    b = int(confidence * 10)
    if b < 0:
        return 0
    if b > 9:
        return 9
    return b


class AuditStore:
    def __init__(self, path: str) -> None:
        self.path = path
        self._conn = sqlite3.connect(path, timeout=60.0, isolation_level=None)
        self._conn.execute("PRAGMA busy_timeout=60000")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "AuditStore":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------ H
    def _bump_h(self, cur: sqlite3.Cursor, ts: float) -> float:
        cur.execute(
            "INSERT INTO meta(k, v) VALUES('h', ?) "
            "ON CONFLICT(k) DO UPDATE SET v = MAX(v, excluded.v)",
            (ts,),
        )
        cur.execute("SELECT v FROM meta WHERE k='h'")
        return float(cur.fetchone()[0])

    def _get_h(self, cur: sqlite3.Cursor) -> float:
        cur.execute("SELECT v FROM meta WHERE k='h'")
        row = cur.fetchone()
        return float(row[0]) if row else 0.0

    # ------------------------------------------------------- aggregate inc
    @staticmethod
    def _inc(cur: sqlite3.Cursor, sql: str, params: tuple) -> None:
        cur.execute(sql, params)

    def _acc_add(self, cur: sqlite3.Cursor, day: int, total: int, uncorrected: int) -> None:
        cur.execute(
            "INSERT INTO agg_acc(day, total, uncorrected) VALUES(?, ?, ?) "
            "ON CONFLICT(day) DO UPDATE SET total = total + ?, uncorrected = uncorrected + ?",
            (day, total, uncorrected, total, uncorrected),
        )

    # ------------------------------------------------------ hot state core
    def _normalize_field(self, cur: sqlite3.Cursor, doc_id: str, field: str, h: float) -> None:
        """Finalize cold corrections/extractions and recompute hot attribution.

        Must be called inside a write transaction whenever the (doc, field)
        state changed or the late-arrival window moved.
        """
        cut = h - LATE_WINDOW_SECONDS
        cur.execute(
            "SELECT ets, day, perm, tmp FROM st_extr WHERE doc_id=? AND field=? ORDER BY ets",
            (doc_id, field),
        )
        rows = cur.fetchall()
        cur.execute(
            "SELECT cts FROM st_corr WHERE doc_id=? AND field=? ORDER BY cts",
            (doc_id, field),
        )
        corrs = [r[0] for r in cur.fetchall()]
        if not rows and not corrs:
            return

        old_corrected = {ets: (perm > 0 or tmp > 0) for ets, _day, perm, tmp in rows}
        perm = {ets: p for ets, _d, p, _t in rows}
        ets_list = [ets for ets, _d, _p, _t in rows]

        def target_of(cts: float, candidates: List[float]) -> float:
            i = bisect_left(candidates, cts)
            return candidates[i - 1] if i > 0 else None  # type: ignore[return-value]

        # Bake cold corrections into permanent flags; their attribution is final.
        hot_corrs = []
        for cts in corrs:
            if cts < cut:
                t = target_of(cts, ets_list)
                if t is not None:
                    perm[t] = 1
            else:
                hot_corrs.append(cts)

        # Collapse cold extractions: keep only the latest one below the window.
        cold = [ets for ets in ets_list if ets < cut]
        dropped = set(cold[:-1]) if len(cold) > 1 else set()
        kept = [ets for ets in ets_list if ets not in dropped]

        # Recompute hot correction attribution over the surviving extractions.
        tmp = {ets: 0 for ets in kept}
        for cts in hot_corrs:
            t = target_of(cts, kept)
            if t is not None:
                tmp[t] += 1

        # Apply accuracy deltas for extractions whose corrected status flipped.
        for ets, day, _p, _t in rows:
            if ets in dropped:
                continue  # already frozen into the aggregate
            new_corrected = perm[ets] > 0 or tmp[ets] > 0
            if old_corrected[ets] != new_corrected:
                self._acc_add(cur, day, 0, -1 if new_corrected else 1)

        # Write the new state back.
        if dropped:
            cur.executemany(
                "DELETE FROM st_extr WHERE doc_id=? AND field=? AND ets=?",
                [(doc_id, field, ets) for ets in dropped],
            )
        cur.executemany(
            "UPDATE st_extr SET perm=?, tmp=? WHERE doc_id=? AND field=? AND ets=?",
            [(perm[ets], tmp[ets], doc_id, field, ets) for ets in kept],
        )
        cold_corrs = [c for c in corrs if c < cut]
        if cold_corrs:
            cur.executemany(
                "DELETE FROM st_corr WHERE doc_id=? AND field=? AND cts=?",
                [(doc_id, field, c) for c in cold_corrs],
            )

    # -------------------------------------------------------------- append
    def append(self, event: Event) -> None:
        cur = self._conn.cursor()
        day = int(event.ts // DAY_SECONDS)
        cur.execute("BEGIN IMMEDIATE")
        try:
            h = self._bump_h(cur, event.ts)
            cur.execute(
                "INSERT INTO events(kind, ts, doc_id, doc_type, field, confidence) "
                "VALUES(?,?,?,?,?,?)",
                (event.kind, event.ts, event.doc_id, event.doc_type, event.field, event.confidence),
            )
            if event.kind == KIND_EXTRACTED:
                cur.execute(
                    "INSERT OR IGNORE INTO doc_day(doc_id, day) VALUES(?,?)",
                    (event.doc_id, day),
                )
                if cur.rowcount:
                    cur.execute(
                        "INSERT INTO agg_vol(day, n) VALUES(?, 1) "
                        "ON CONFLICT(day) DO UPDATE SET n = n + 1",
                        (day,),
                    )
                cur.execute(
                    "INSERT INTO agg_hist(day, doc_type, bucket, n) VALUES(?,?,?,1) "
                    "ON CONFLICT(day, doc_type, bucket) DO UPDATE SET n = n + 1",
                    (day, event.doc_type, _bucket(event.confidence or 0.0)),
                )
                cur.execute(
                    "INSERT INTO st_extr(doc_id, field, ets, day) VALUES(?,?,?,?)",
                    (event.doc_id, event.field, event.ts, day),
                )
                self._acc_add(cur, day, 1, 1)
                self._normalize_field(cur, event.doc_id, event.field, h)
            elif event.kind == KIND_CORRECTED:
                cur.execute(
                    "INSERT INTO st_corr(doc_id, field, cts) VALUES(?,?,?)",
                    (event.doc_id, event.field, event.ts),
                )
                self._normalize_field(cur, event.doc_id, event.field, h)
            elif event.kind == KIND_APPROVED:
                cur.execute("DELETE FROM st_extr WHERE doc_id=?", (event.doc_id,))
                cur.execute("DELETE FROM st_corr WHERE doc_id=?", (event.doc_id,))
            cur.execute("COMMIT")
        except BaseException:
            cur.execute("ROLLBACK")
            raise

    # ------------------------------------------------------------- queries
    def daily_volume(self, day: int) -> int:
        row = self._conn.execute("SELECT n FROM agg_vol WHERE day=?", (day,)).fetchone()
        return int(row[0]) if row else 0

    def confidence_histogram(self, day: int, doc_type: str) -> List[int]:
        out = [0] * 10
        for bucket, n in self._conn.execute(
            "SELECT bucket, n FROM agg_hist WHERE day=? AND doc_type=?", (day, doc_type)
        ):
            out[bucket] = n
        return out

    def accuracy(self, day: int) -> Tuple[int, int]:
        row = self._conn.execute(
            "SELECT total, uncorrected FROM agg_acc WHERE day=?", (day,)
        ).fetchone()
        return (int(row[0]), int(row[1])) if row else (0, 0)

    # -------------------------------------------------------------- prune
    def prune(self, now: float) -> bool:
        """Run one bounded batch of pruning. Returns True if any work was done."""
        cur = self._conn.cursor()
        cur.execute("BEGIN IMMEDIATE")
        try:
            h = self._bump_h(cur, now)
            cutoff = now - RETENTION_SECONDS
            cur.execute(
                "DELETE FROM events WHERE rowid IN "
                "(SELECT rowid FROM events WHERE ts < ? LIMIT ?)",
                (cutoff, PRUNE_BATCH),
            )
            n_events = cur.rowcount

            # doc_day rows are only needed while late arrivals for that day
            # are still possible.
            cur.execute(
                "DELETE FROM doc_day WHERE rowid IN "
                "(SELECT rowid FROM doc_day WHERE (day + 1) * ? <= ? LIMIT ?)",
                (DAY_SECONDS, h - LATE_WINDOW_SECONDS, PRUNE_BATCH),
            )
            n_docday = cur.rowcount

            # Collapse hot state for fields not touched recently.
            pairs: List[Tuple[str, str]] = []
            cur.execute(
                "SELECT DISTINCT doc_id, field FROM st_corr WHERE cts < ? LIMIT ?",
                (h - LATE_WINDOW_SECONDS, STATE_COLLAPSE_BATCH),
            )
            pairs.extend((r[0], r[1]) for r in cur.fetchall())
            if len(pairs) < STATE_COLLAPSE_BATCH:
                cur.execute(
                    "SELECT doc_id, field FROM st_extr WHERE ets < ? "
                    "GROUP BY doc_id, field HAVING COUNT(*) > 1 LIMIT ?",
                    (h - LATE_WINDOW_SECONDS, STATE_COLLAPSE_BATCH - len(pairs)),
                )
                pairs.extend((r[0], r[1]) for r in cur.fetchall())
            for doc_id, field in pairs:
                self._normalize_field(cur, doc_id, field, h)

            cur.execute("COMMIT")
        except BaseException:
            cur.execute("ROLLBACK")
            raise
        return bool(n_events or n_docday or pairs)

    def prune_all(self, now: float) -> None:
        while self.prune(now):
            pass

    # ------------------------------------------------------------- helpers
    def count_raw_events(self) -> int:
        return int(self._conn.execute("SELECT COUNT(*) FROM events").fetchone()[0])

    def count_state_rows(self) -> Tuple[int, int]:
        a = int(self._conn.execute("SELECT COUNT(*) FROM st_extr").fetchone()[0])
        b = int(self._conn.execute("SELECT COUNT(*) FROM st_corr").fetchone()[0])
        return a, b
