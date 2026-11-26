"""In-memory batch records (the drill is single-process)."""
from __future__ import annotations

import threading
from collections import OrderedDict
from typing import Any


class BatchStore:
    def __init__(self, limit: int = 200) -> None:
        self._lock = threading.RLock()
        self._items: "OrderedDict[str, dict[str, Any]]" = OrderedDict()
        self._limit = limit

    def add(self, record: dict[str, Any]) -> None:
        with self._lock:
            self._items[record["batch_id"]] = record
            self._items.move_to_end(record["batch_id"])
            while len(self._items) > self._limit:
                self._items.popitem(last=False)

    def update(self, record: dict[str, Any]) -> None:
        self.add(record)

    def get(self, batch_id: str) -> dict[str, Any] | None:
        with self._lock:
            rec = self._items.get(batch_id)
            return dict(rec) if rec is not None else None

    def list(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._lock:
            out = []
            for bid in reversed(self._items.keys()):
                rec = self._items[bid]
                out.append(_summary(rec))
                if len(out) >= limit:
                    break
            return out


def _summary(rec: dict[str, Any]) -> dict[str, Any]:
    return {
        "batch_id": rec["batch_id"],
        "status": rec["status"],
        "accepted": rec.get("accepted"),
        "query_count": len(rec.get("snapshot", [])),
        "max_prepares": rec.get("max_prepares"),
        "planned_prepares": rec.get("planned_prepares"),
        "actual_prepares": rec.get("actual_prepares"),
        "created_at": rec.get("created_at"),
        "finished_at": rec.get("finished_at"),
        "error": (rec.get("error") or {}).get("message") if rec.get("error") else None,
    }
