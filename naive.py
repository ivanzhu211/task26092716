"""In-memory naive implementation used as the test oracle.

Keeps every event forever and computes each query directly from the full
event set.  Any correct implementation must agree with this one at all
times, regardless of pruning.
"""

from __future__ import annotations

from bisect import bisect_left
from collections import defaultdict

from audit import Event, day_of


class NaiveAudit:
    def __init__(self) -> None:
        self.events: list[Event] = []

    def append(self, event: Event) -> None:
        self.events.append(event)

    def daily_volume(self, day: int) -> int:
        return len({
            e.doc_id for e in self.events
            if e.kind == "extracted" and day_of(e.ts) == day
        })

    def confidence_histogram(self, day: int, doc_type: str) -> list[int]:
        result = [0] * 10
        for e in self.events:
            if e.kind == "extracted" and day_of(e.ts) == day and e.doc_type == doc_type:
                b = int(e.confidence * 10)
                result[9 if b > 9 else max(b, 0)] += 1
        return result

    def accuracy(self, day: int) -> tuple[int, int]:
        # Group extractions and corrections by (doc_id, field).
        ext_ts: dict[tuple[str, str], list[int]] = defaultdict(list)
        corr_ts: dict[tuple[str, str], list[int]] = defaultdict(list)
        for e in self.events:
            if e.kind == "extracted":
                ext_ts[(e.doc_id, e.field)].append(e.ts)
            elif e.kind == "corrected":
                corr_ts[(e.doc_id, e.field)].append(e.ts)
        corrected_ext: set[tuple[str, str, int]] = set()
        for key, cts_list in corr_ts.items():
            ets = sorted(ext_ts.get(key, []))
            for cts in cts_list:
                idx = bisect_left(ets, cts) - 1
                if idx >= 0:
                    corrected_ext.add((key[0], key[1], ets[idx]))
        total = 0
        corrected = 0
        for e in self.events:
            if e.kind == "extracted" and day_of(e.ts) == day:
                total += 1
                if (e.doc_id, e.field, e.ts) in corrected_ext:
                    corrected += 1
        return (total, total - corrected)
