"""Full concurrency test for the audit store.

Scenario (all timestamps relative to BASE, in days):
  - historical events span days 0..449, appended by the parent up front
    (in an order that is shuffled within the 72h late-arrival window);
  - a pruner subprocess runs prune(now = BASE + 460.5d) in batches and is
    SIGKILLed at random several times, then restarted;
  - an appender subprocess concurrently appends events for brand-new docs
    (days 460..470);
  - a querier subprocess continuously snapshots historical days and
    compares against a naive in-memory implementation over exactly the
    events appended so far; any mismatch fails the test.

Required edge cases covered by the generated stream:
  * same doc extracted several times on day 95, with the prune cutoff
    (BASE + 95.5d) slicing that day in half;
  * an extraction on day 5 corrected on day 440 (long past retention);
  * re-extraction followed by a correction (correction counts once, on
    the latest extraction);
  - out-of-order arrivals that change correction attribution, in both
    directions (correction before its extraction; a later extraction
    landing between an earlier extraction and its correction).
"""

from __future__ import annotations

import multiprocessing as mp
import os
import pickle
import random
import sqlite3
import sys
import tempfile
import time

from audit import AuditStore, Event, day_of, SECONDS_PER_DAY
from naive import NaiveAudit

BASE = 1_700_000_000
DAY = SECONDS_PER_DAY
HOUR = 3600
NOW = BASE + 460 * DAY + 12 * HOUR          # prune "now": cutoff = BASE + 95.5d
CUTOFF = NOW - 365 * DAY
DOC_TYPES = ["invoice", "receipt", "contract"]


# ---------------------------------------------------------------------
# event stream generation (deterministic)
# ---------------------------------------------------------------------
def build_stream():
    rng = random.Random(20260927)
    events = []          # (event, tag); tag used for forced reordering

    # -- cutoff day: two docs, several extractions each, day sliced by cutoff
    for doc in ("cut-a", "cut-b"):
        for i, (h, conf) in enumerate([(2, 0.05), (10, 0.5), (14, 0.95), (20, 1.0)]):
            events.append((Event("extracted", BASE + 95 * DAY + h * HOUR + i,
                                 doc, "invoice", "amount", conf), None))
    # -- ancient extraction corrected long after retention
    events.append((Event("extracted", BASE + 5 * DAY + 3 * HOUR,
                         "ancient", "invoice", "total", 0.5), None))
    events.append((Event("corrected", BASE + 440 * DAY + 6 * HOUR,
                         "ancient", None, "total", None), None))
    # -- ancient2: same but approved afterwards (state must be finalized)
    events.append((Event("extracted", BASE + 10 * DAY,
                         "ancient2", "receipt", "total", 0.6), None))
    events.append((Event("corrected", BASE + 445 * DAY + HOUR,
                         "ancient2", None, "total", None), None))
    events.append((Event("approved", BASE + 446 * DAY,
                         "ancient2", None, None, None), None))
    # -- re-extraction then correction: only the latest extraction counts
    events.append((Event("extracted", BASE + 200 * DAY + 10 * HOUR,
                         "reext", "contract", "amount", 0.4), None))
    events.append((Event("extracted", BASE + 200 * DAY + 12 * HOUR,
                         "reext", "contract", "amount", 0.9), None))
    events.append((Event("corrected", BASE + 200 * DAY + 14 * HOUR,
                         "reext", None, "amount", None), None))
    # -- out-of-order: correction, then an extraction that steals it
    events.append((Event("extracted", BASE + 210 * DAY + 23 * HOUR,
                         "ooo", "invoice", "vendor", 0.7), "ooo-e1"))
    events.append((Event("corrected", BASE + 211 * DAY + 3 * HOUR,
                         "ooo", None, "vendor", None), "ooo-c"))
    events.append((Event("extracted", BASE + 211 * DAY + HOUR,
                         "ooo", "invoice", "vendor", 0.8), "ooo-e2"))
    # -- out-of-order: correction arrives before its extraction
    events.append((Event("corrected", BASE + 220 * DAY + 5 * HOUR,
                         "late-ext", None, "date", None), "le-c"))
    events.append((Event("extracted", BASE + 220 * DAY + 4 * HOUR,
                         "late-ext", "receipt", "date", 0.66), "le-e"))

    # -- filler docs: 60% in the pruned range so prune needs many batches
    for i in range(2600):
        doc = f"fill-{i}"
        dtype = rng.choice(DOC_TYPES)
        field = rng.choice(["amount", "date", "vendor"])
        if rng.random() < 0.6:
            day = rng.randrange(0, 90)
        else:
            day = rng.randrange(91, 449)
        t = BASE + day * DAY + rng.randrange(0, DAY - 4 * HOUR)
        used = set()
        n_ext = 1 + (rng.random() < 0.3)
        last = 0
        for _ in range(n_ext):
            t += rng.randrange(1, 7200)
            while t in used:
                t += 1
            used.add(t)
            events.append((Event("extracted", t, doc, dtype, field,
                                 round(rng.random(), 3)), None))
            last = t
        if rng.random() < 0.5:
            t = last + rng.randrange(1, 90000)
            while t in used:
                t += 1
            used.add(t)
            events.append((Event("corrected", t, doc, None, field, None), None))
            last = t
        if rng.random() < 0.5:
            t = last + rng.randrange(1, 90000)
            events.append((Event("approved", t, doc, None, None, None), None))

    # sort, shuffle inside the 72h window, then force the tagged inversions
    events.sort(key=lambda p: p[0].ts)
    order, buf = [], []
    for p in events:
        buf.append(p)
        while buf and buf[0][0].ts < p[0].ts - 70 * HOUR:
            order.append(buf.pop(0))
        if buf and rng.random() < 0.4:
            order.append(buf.pop(rng.randrange(len(buf))))
    order.extend(buf)

    def move_after(tag, after_tag):
        nonlocal order
        item = next(p for p in order if p[1] == tag)
        order.remove(item)
        idx = next(i for i, p in enumerate(order) if p[1] == after_tag)
        order.insert(idx + 1, item)

    def move_before(tag, before_tag):
        nonlocal order
        item = next(p for p in order if p[1] == tag)
        order.remove(item)
        idx = next(i for i, p in enumerate(order) if p[1] == before_tag)
        order.insert(idx, item)

    move_after("ooo-e2", "ooo-c")     # extraction lands after its correction
    move_before("le-c", "le-e")       # correction arrives before extraction
    historical = [p[0] for p in order]

    # -- new-doc events appended concurrently by the appender subprocess
    new_events = []
    t = NOW + HOUR
    for i in range(200):
        doc = f"new-{i}"
        dtype = DOC_TYPES[i % 3]
        t += rng.randrange(HOUR, 6 * HOUR)
        new_events.append(Event("extracted", t, doc, dtype, "amount",
                                round(rng.random(), 3)))
        if rng.random() < 0.5:
            t += rng.randrange(1, 3600)
            new_events.append(Event("corrected", t, doc, None, "amount", None))
        if rng.random() < 0.5:
            t += rng.randrange(1, 3600)
            new_events.append(Event("approved", t, doc, None, None, None))
    return historical, new_events


# ---------------------------------------------------------------------
# subprocess workers
# ---------------------------------------------------------------------
def pruner_worker(db_path, now):
    store = AuditStore(db_path)
    store.prune(now, batch_sleep=0.1)  # slow batches: widen the kill window
    store.close()


def appender_worker(db_path, events_file, n_historical):
    with open(events_file, "rb") as fh:
        _, new_events = pickle.load(fh)
    store = AuditStore(db_path)
    rng = random.Random(1)
    for e in new_events:
        store.append(e)
        time.sleep(rng.random() * 0.004)
    store.close()


def querier_worker(db_path, events_file, stop_file, fail_file):
    with open(events_file, "rb") as fh:
        historical, new_events = pickle.load(fh)
    all_events = historical + new_events
    store = AuditStore(db_path)
    rng = random.Random(2)
    interesting = [5, 10, 95, 200, 210, 211, 220, 440, 445, 446]
    naive = NaiveAudit()
    count = 0
    try:
        while count < 60 or not os.path.exists(stop_file):
            count += 1
            if count > 4000:
                break
            day = rng.choice(interesting) if rng.random() < 0.5 else rng.randrange(0, 472)
            dtype = rng.choice(DOC_TYPES)
            k, vol, hist, acc = store.snapshot(day, dtype)
            for e in all_events[len(naive.events):k]:
                naive.append(e)
            # naive may only move forward; k is nondecreasing per snapshot
            if k < len(naive.events):
                naive = NaiveAudit()
                for e in all_events[:k]:
                    naive.append(e)
            exp = (naive.daily_volume(day), naive.confidence_histogram(day, dtype),
                   naive.accuracy(day))
            got = (vol, hist, acc)
            if got != exp:
                with open(fail_file, "a") as fh:
                    fh.write(f"day={day} type={dtype} k={k} got={got} want={exp}\n")
                os._exit(1)
            time.sleep(rng.random() * 0.003)
    finally:
        store.close()
    os._exit(0)


# ---------------------------------------------------------------------
# main
# ---------------------------------------------------------------------
def main():
    tmp = tempfile.mkdtemp(prefix="audit-test-")
    db_path = os.path.join(tmp, "audit.db")
    events_file = os.path.join(tmp, "events.pkl")
    stop_file = os.path.join(tmp, "stop")
    fail_file = os.path.join(tmp, "fail")

    historical, new_events = build_stream()
    all_events = historical + new_events
    with open(events_file, "wb") as fh:
        pickle.dump((historical, new_events), fh)

    # Phase 1: append historical events.
    store = AuditStore(db_path)
    t0 = time.time()
    for e in historical:
        store.append(e)
    print(f"historical appended: {len(historical)} events in {time.time()-t0:.1f}s")

    ctx = mp.get_context("spawn")
    querier = ctx.Process(target=querier_worker,
                          args=(db_path, events_file, stop_file, fail_file))
    appender = ctx.Process(target=appender_worker,
                           args=(db_path, events_file, len(historical)))
    querier.start()
    appender.start()

    # Phase 2: prune with random kills and restarts.
    kills = 0
    pruner = ctx.Process(target=pruner_worker, args=(db_path, NOW))
    pruner.start()
    rng = random.Random(3)
    while True:
        time.sleep(rng.random() * 0.4 + 0.1)
        if pruner.is_alive():
            if kills < 4 and rng.random() < 0.7:
                pruner.kill()
                pruner.join()
                kills += 1
                print(f"pruner killed ({kills}), restarting")
                pruner = ctx.Process(target=pruner_worker, args=(db_path, NOW))
                pruner.start()
        else:
            pruner.join()
            if pruner.exitcode == 0:
                break
            print(f"pruner exited with {pruner.exitcode}, restarting")
            pruner = ctx.Process(target=pruner_worker, args=(db_path, NOW))
            pruner.start()

    appender.join()
    open(stop_file, "w").write("done")
    querier.join()
    print(f"prune done after {kills} kills; appender exit={appender.exitcode}, "
          f"querier exit={querier.exitcode}")

    if os.path.exists(fail_file):
        print("QUERIER MISMATCH:")
        print(open(fail_file).read())
        sys.exit(1)
    if appender.exitcode != 0 or querier.exitcode != 0:
        print("worker failed")
        sys.exit(1)

    # Phase 3: final full verification against the naive oracle.
    naive = NaiveAudit()
    for e in all_events:
        naive.append(e)
    bad = 0
    for day in range(0, 472):
        if store.daily_volume(day) != naive.daily_volume(day):
            print("volume mismatch", day); bad += 1
        if store.accuracy(day) != naive.accuracy(day):
            print("accuracy mismatch", day,
                  store.accuracy(day), naive.accuracy(day)); bad += 1
        for dt in DOC_TYPES:
            if store.confidence_histogram(day, dt) != naive.confidence_histogram(day, dt):
                print("histogram mismatch", day, dt); bad += 1
    assert bad == 0, f"{bad} mismatches"

    # Raw events older than the cutoff must be physically gone.
    con = sqlite3.connect(db_path)
    old = con.execute("SELECT COUNT(*) FROM events WHERE ts < ?", (CUTOFF,)).fetchone()[0]
    kept = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    want_kept = sum(1 for e in all_events if e.ts >= CUTOFF)
    print(f"raw events: {old} old (want 0), {kept} kept (want {want_kept})")
    assert old == 0 and kept == want_kept

    # Final prune far in the future: all derived per-event state must be
    # bounded (empty, except attribution state for never-approved docs).
    store.prune(NOW + 40 * DAY)
    daily_docs = con.execute("SELECT COUNT(*) FROM daily_docs").fetchone()[0]
    docs_pending = con.execute("SELECT COUNT(*) FROM docs").fetchone()[0]
    pend_corr = con.execute("SELECT COUNT(*) FROM pend_corr").fetchone()[0]
    pend_ext = con.execute("SELECT COUNT(*) FROM pend_ext").fetchone()[0]
    dup = con.execute(
        "SELECT COUNT(*) FROM (SELECT doc_id, field FROM pend_ext"
        " GROUP BY doc_id, field HAVING COUNT(*) > 1)").fetchone()[0]
    print(f"residual state: daily_docs={daily_docs} docs={docs_pending} "
          f"pend_corr={pend_corr} pend_ext={pend_ext} dup_fields={dup}")
    assert daily_docs == 0 and docs_pending == 0 and pend_corr == 0 and dup == 0
    # every remaining pend_ext row belongs to a doc that was never approved
    approved_docs = {e.doc_id for e in all_events if e.kind == "approved"}
    leftover = con.execute("SELECT DISTINCT doc_id FROM pend_ext").fetchall()
    assert all(d not in approved_docs for (d,) in leftover), leftover[:5]

    store.close()
    con.close()
    print(f"TEST_OK ({len(all_events)} events, {kills} pruner kills)")


if __name__ == "__main__":
    main()
