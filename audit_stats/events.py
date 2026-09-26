"""Event types for the audit statistics pipeline."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

KIND_EXTRACTED = "extracted"
KIND_CORRECTED = "corrected"
KIND_APPROVED = "approved"


@dataclass(frozen=True)
class Event:
    kind: str
    ts: float
    doc_id: str
    doc_type: Optional[str] = None
    field: Optional[str] = None
    confidence: Optional[float] = None


def extracted(ts: float, doc_id: str, doc_type: str, field: str, confidence: float) -> Event:
    return Event(KIND_EXTRACTED, ts, doc_id, doc_type=doc_type, field=field, confidence=confidence)


def corrected(ts: float, doc_id: str, field: str) -> Event:
    return Event(KIND_CORRECTED, ts, doc_id, field=field)


def approved(ts: float, doc_id: str) -> Event:
    return Event(KIND_APPROVED, ts, doc_id)
