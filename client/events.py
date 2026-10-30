"""Nhật ký sự kiện cho giao diện web. Trình duyệt polling GET /api/events?since=<id>.

Trạng thái: HOÀN THIỆN.
Các loại sự kiện (kind): status, users, user_join, user_leave, message, error,
                         file_incoming, file_progress, file_done
"""
import time
from collections import deque

MAX_EVENTS = 5000


class EventLog:
    def __init__(self) -> None:
        self._events: deque[dict] = deque(maxlen=MAX_EVENTS)
        self._next_id = 1

    def add(self, kind: str, **data) -> dict:
        ev = {"id": self._next_id, "kind": kind, "ts": time.time(), **data}
        self._next_id += 1
        self._events.append(ev)
        return ev

    def since(self, last_id: int, limit: int = 500) -> list[dict]:
        out = [e for e in self._events if e["id"] > last_id]
        return out[:limit]

    @property
    def last_id(self) -> int:
        return self._next_id - 1
