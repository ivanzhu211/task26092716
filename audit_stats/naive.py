"""In-memory naive reference implementation.

Keeps every event forever and answers queries by brute force.  Used as the
ground truth that the SQLite store must match at all times.
"""
from __future__ import annotations

from typing import Dict, List, Set, Tuple

from .events import KIND_APPROVED, KIND_CORRECTED, KIND_EXTRACTED, Event
from .store import DAY_SECONDS


class NaiveAudit:
    def __init__(self) -> None:
        self.events: List[Event] = []

    def append(self, event: Event) -> None:
        self.events.append(event)

    def daily_volume(self, day: int) -> int:
        return len(
            {
                e.doc_id
                for e in self.events
                if e.kind == KIND_EXTRACTED and int(e.ts // DAY_SECONDS) == day
            }
        )

    def confidence_histogram(self, day: int, doc_type: str) -> List[int]:
        out = [0] * 10
        for e in self.events:
            if (
                e.kind == KIND_EXTRACTED
                and int(e.ts // DAY_SECONDS) == day
                and e.doc_type == doc_type
            ):
                b = int((e.confidence or 0.0) * 10)
                out[min(max(b, 0), 9)] += 1
        return out

    def accuracy(self, day: int) -> Tuple[int, int]:
        extractions: Dict[Tuple[str, str], List[float]] = {}
        corrections: List[Event] = []
        for e in self.events:
            if e.kind == KIND_EXTRACTED:
                extractions.setdefault((e.doc_id, e.field), []).append(e.ts)
            elif e.kind == KIND_CORRECTED:
                corrections.append(e)

        corrected_extractions: Set[Tuple[str, str, float]] = set()
        for c in corrections:
            cands = [ts for ts in extractions.get((c.doc_id, c.field), []) if ts < c.ts]
            if cands:
                corrected_extractions.add((c.doc_id, c.field, max(cands)))

        total = 0
        uncorrected = 0
        for e in self.events:
            if e.kind == KIND_EXTRACTED and int(e.ts // DAY_SECONDS) == day:
                total += 1
                if (e.doc_id, e.field, e.ts) not in corrected_extractions:
                    uncorrected += 1
        return total, uncorrected
