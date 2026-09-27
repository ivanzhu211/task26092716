"""Single-process randomized consistency test: store vs naive oracle."""
import os
import random
import sys
import tempfile

from audit import AuditStore, Event, day_of, SECONDS_PER_DAY
from naive import NaiveAudit

BASE = 1_700_000_000
DAY = SECONDS_PER_DAY


def gen_stream(rng, n_docs, span_days):
    """Random events; returned in an append order respecting the 72h rule."""
    events = []
    for i in range(n_docs):
        doc = f"doc-{i}"
        dtype = rng.choice(["invoice", "receipt", "contract"])
        field = rng.choice(["amount", "date", "vendor"])
        t = BASE + rng.randrange(0, span_days) * DAY + rng.randrange(0, DAY - 40000)
        used = set()
        n_ext = 1 + (rng.random() < 0.3)
        last = 0
        for _ in range(n_ext):
            t += rng.randrange(1, 7200)
            while t in used:
                t += 1
            used.add(t)
            events.append(Event("extracted", t, doc, dtype, field, rng.random()))
            last = t
        if rng.random() < 0.5:
            t = last + rng.randrange(1, 90000)
            while t in used:
                t += 1
            used.add(t)
            events.append(Event("corrected", t, doc, None, field, None))
            last = t
        if rng.random() < 0.5:
            t = last + rng.randrange(1, 90000)
            events.append(Event("approved", t, doc))
    events.sort(key=lambda e: e.ts)
    # Shuffle within the 72h late-arrival window.
    order, buf = [], []
    for e in events:
        buf.append(e)
        while buf and buf[0].ts < e.ts - 70 * 3600:
            order.append(buf.pop(0))
        if buf and rng.random() < 0.4:
            order.append(buf.pop(rng.randrange(len(buf))))
    order.extend(buf)
    return order


def check_all(store, naive, span_days, doc_types):
    for day in range(span_days + 2):
        assert store.daily_volume(day) == naive.daily_volume(day), \
            (day, store.daily_volume(day), naive.daily_volume(day))
        assert store.accuracy(day) == naive.accuracy(day), \
            (day, store.accuracy(day), naive.accuracy(day))
        for dt in doc_types:
            assert store.confidence_histogram(day, dt) == naive.confidence_histogram(day, dt), \
                (day, dt)


def main():
    rng = random.Random(7)
    path = tempfile.mktemp(suffix=".db")
    store = AuditStore(path)
    naive = NaiveAudit()
    span_days = 500
    stream = gen_stream(rng, 400, span_days - 60)
    # Append everything, then prune in two rounds with advancing `now`.
    for e in stream:
        store.append(e)
        naive.append(e)
    check_all(store, naive, span_days, ["invoice", "receipt", "contract"])
    store.prune(BASE + 300 * DAY)
    check_all(store, naive, span_days, ["invoice", "receipt", "contract"])
    store.prune(BASE + 470 * DAY)
    check_all(store, naive, span_days, ["invoice", "receipt", "contract"])
    # Late appends after pruning (new docs only, ts near horizon).
    h = BASE + 470 * DAY
    for i in range(20):
        doc = f"late-{i}"
        t = h + i * 3600
        store.append(Event("extracted", t, doc, "invoice", "amount", 0.95))
        naive.append(Event("extracted", t, doc, "invoice", "amount", 0.95))
    check_all(store, naive, span_days, ["invoice", "receipt", "contract"])
    store.close()
    os.unlink(path)
    for suffix in ("-wal", "-shm"):
        if os.path.exists(path + suffix):
            os.unlink(path + suffix)
    print("BASIC_OK")


if __name__ == "__main__":
    main()
