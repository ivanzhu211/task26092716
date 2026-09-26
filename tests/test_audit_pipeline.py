"""End-to-end stress test for the audit statistics pipeline.

Generates a >400 day event stream containing the required tricky scenarios,
then runs three concurrent processes against the SQLite store:

* a pruner that works in bounded batches and is randomly SIGKILLed and
  restarted several times;
* an appender that keeps adding events for brand-new documents;
* a querier that continuously compares historical-day query results against
  a pure in-memory naive implementation fed the same events.

Any mismatch fails the test.
"""
from __future__ import annotations

import multiprocessing as mp
import os
import random
import shutil
import signal
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from audit_stats import AuditStore, NaiveAudit, approved, corrected, extracted
from audit_stats.events import KIND_APPROVED, KIND_EXTRACTED
from audit_stats.store import DAY_SECONDS, LATE_WINDOW_SECONDS, RETENTION_SECONDS

DOC_TYPES = ("invoice", "receipt", "contract")
NOW = 400 * DAY_SECONDS + 12 * 3600  # day 400, noon -> cutoff lands mid day 35
CUTOFF = NOW - RETENTION_SECONDS


def build_main_events(rng: random.Random):
    """Deterministic event stream, returned in append order (with bounded
    out-of-order inversions that respect the 72h lateness rule)."""
    events = []

    # Scenario A: same doc extracted twice on day 35; the prune cutoff
    # (day 35 noon) slices right through that day.
    events.append(extracted(35 * DAY_SECONDS + 6 * 3600, "A", "receipt", "amount", 0.95))
    events.append(extracted(35 * DAY_SECONDS + 18 * 3600, "A", "receipt", "amount", 0.5))
    events.append(extracted(35 * DAY_SECONDS + 3 * 3600, "A2", "receipt", "amount", 0.05))
    events.append(extracted(35 * DAY_SECONDS + 20 * 3600, "A2", "receipt", "amount", 1.0))

    # Scenario B: extracted ~400 days ago, corrected only last week.
    events.append(extracted(1 * DAY_SECONDS + 123.0, "B", "invoice", "total", 0.3))
    events.append(corrected(393 * DAY_SECONDS + 456.0, "B", "total"))

    # Scenario C: re-extraction after a ruleset change, then a correction
    # that must attach to the most recent extraction only.
    events.append(extracted(100 * DAY_SECONDS + 10.0, "C", "invoice", "date", 0.4))
    events.append(extracted(100 * DAY_SECONDS + 3610.0, "C", "invoice", "date", 0.9))
    events.append(corrected(100 * DAY_SECONDS + 7210.0, "C", "date"))

    # Scenario D: out-of-order arrival that changes correction attribution.
    # Append order is e1, correction, e2 (e2's ts sits between them).
    events.append(extracted(200 * DAY_SECONDS + 100.0, "D", "invoice", "total", 0.6))
    events.append(extracted(200 * DAY_SECONDS + 3700.0, "D", "invoice", "total", 0.7))
    events.append(corrected(200 * DAY_SECONDS + 7300.0, "D", "total"))

    # Bulk random traffic across the whole 400 days.
    bulk_docs = [f"doc-{i}" for i in range(120)]
    for _ in range(4000):
        d = rng.randint(0, 399)
        doc = rng.choice(bulk_docs)
        field = rng.choice(("f1", "f2"))
        events.append(
            extracted(
                d * DAY_SECONDS + rng.random() * DAY_SECONDS,
                doc,
                rng.choice(DOC_TYPES),
                field,
                rng.random(),
            )
        )
        if rng.random() < 0.25:
            events.append(
                corrected(d * DAY_SECONDS + rng.random() * DAY_SECONDS, doc, field)
            )

    # Approvals: last event (arrival and timestamp) for their document.
    approved_docs = {"A", "A2", "C"} | set(bulk_docs[::2])
    for doc in approved_docs:
        events.append(approved(NOW - 3600 + rng.random() * 60, doc))

    events.sort(key=lambda e: e.ts)

    # Bounded out-of-order shuffling: adjacent swaps with < 24h ts distance.
    # Never let a document's own event jump past its `approved` event, and
    # force scenario D's inversion (e2 must arrive after the correction).
    evts = list(events)
    i = 0
    while i < len(evts) - 1:
        a, b = evts[i], evts[i + 1]
        same_doc = a.doc_id == b.doc_id
        swappable = abs(a.ts - b.ts) < 24 * 3600 and not (
            same_doc and (a.kind == KIND_APPROVED or b.kind == KIND_APPROVED)
        )
        forced = a.doc_id == "D" and a.kind == KIND_EXTRACTED and b.doc_id == "D"
        if swappable and (forced or rng.random() < 0.15):
            evts[i], evts[i + 1] = b, a
            i += 2
        else:
            i += 1
    return evts


def build_live_events(rng: random.Random):
    """Events for brand-new documents, appended concurrently with pruning."""
    out = []
    for i in range(120):
        doc = f"live-{i}"
        ts = NOW - 1800 + i * 2.0
        out.append(extracted(ts, doc, "invoice", "f1", rng.random()))
        out.append(approved(ts + 1.0, doc))
    return out


def prune_worker(db_path: str, now: float) -> None:
    store = AuditStore(db_path)
    try:
        while store.prune(now):
            time.sleep(0.1)  # simulate a long-running prune over big data
    finally:
        store.close()


def append_worker(db_path: str, live_events) -> None:
    store = AuditStore(db_path)
    try:
        for e in live_events:
            store.append(e)
            time.sleep(0.001)
    finally:
        store.close()


def query_worker(db_path: str, main_events, iterations: int, seed: int) -> None:
    rng = random.Random(seed)
    naive = NaiveAudit()
    for e in main_events:
        naive.append(e)
    store = AuditStore(db_path)
    try:
        for _ in range(iterations):
            day = rng.randint(0, 394)  # historical days only; live events are day 400
            assert store.daily_volume(day) == naive.daily_volume(day), ("vol", day)
            assert store.accuracy(day) == naive.accuracy(day), ("acc", day)
            dt = rng.choice(DOC_TYPES)
            got = store.confidence_histogram(day, dt)
            want = naive.confidence_histogram(day, dt)
            assert got == want, ("hist", day, dt)
            time.sleep(rng.uniform(0.0, 0.005))
    finally:
        store.close()


class AuditPipelineTest(unittest.TestCase):
    def test_concurrent_prune_append_query(self) -> None:
        rng = random.Random(20260927)
        main_events = build_main_events(rng)
        live_events = build_live_events(random.Random(99))

        tmpdir = tempfile.mkdtemp(prefix="audit_test_")
        self.addCleanup(shutil.rmtree, tmpdir, True)
        db_path = os.path.join(tmpdir, "audit.db")

        store = AuditStore(db_path)
        naive = NaiveAudit()
        for e in main_events:
            store.append(e)
            naive.append(e)
        for e in live_events:
            naive.append(e)

        querier = mp.Process(
            target=query_worker, args=(db_path, main_events, 150, 1234)
        )
        appender = mp.Process(target=append_worker, args=(db_path, live_events))
        querier.start()
        appender.start()

        # Prune in a subprocess, randomly SIGKILLing and restarting it.
        kills = 0
        done = False
        while not done:
            p = mp.Process(target=prune_worker, args=(db_path, NOW))
            p.start()
            time.sleep(rng.uniform(0.05, 0.3))
            if p.is_alive() and kills < 4 and rng.random() < 0.75:
                os.kill(p.pid, signal.SIGKILL)
                kills += 1
            p.join()
            done = not store.prune(NOW)  # finish remaining batches in-process
        store.prune_all(NOW)

        appender.join(timeout=60)
        querier.join(timeout=120)
        self.assertEqual(appender.exitcode, 0, "appender failed")
        self.assertEqual(querier.exitcode, 0, "querier found a mismatch")

        # Final full verification against the naive implementation.
        for day in range(0, 401):
            self.assertEqual(store.daily_volume(day), naive.daily_volume(day), ("vol", day))
            self.assertEqual(store.accuracy(day), naive.accuracy(day), ("acc", day))
            for dt in DOC_TYPES:
                self.assertEqual(
                    store.confidence_histogram(day, dt),
                    naive.confidence_histogram(day, dt),
                    ("hist", day, dt),
                )

        # Prune must have physically deleted exactly the expired raw events.
        survivors = sum(1 for e in main_events + live_events if e.ts >= CUTOFF)
        self.assertEqual(store.count_raw_events(), survivors)
        self.assertGreater(kills, 0, "pruner was never killed")

        # Hot state must be minimal: only unapproved docs keep one boundary
        # extraction per (doc, field) plus whatever is inside the 72h window.
        h = NOW
        expected_state = 0
        fields = {}
        expected_corr = 0
        approved_ids = {e.doc_id for e in main_events + live_events if e.kind == KIND_APPROVED}
        for e in main_events + live_events:
            if e.doc_id in approved_ids:
                continue
            if e.kind == KIND_EXTRACTED:
                fields.setdefault((e.doc_id, e.field), []).append(e.ts)
            elif e.kind == "corrected" and e.ts >= h - LATE_WINDOW_SECONDS:
                expected_corr += 1
        for ets_list in fields.values():
            hot = sum(1 for t in ets_list if t >= h - LATE_WINDOW_SECONDS)
            cold = len(ets_list) - hot
            expected_state += hot + (1 if cold else 0)
        n_extr, n_corr = store.count_state_rows()
        self.assertEqual(n_corr, expected_corr)
        self.assertEqual(n_extr, expected_state)

        store.close()


if __name__ == "__main__":
    unittest.main()
