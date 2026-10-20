from __future__ import annotations

import threading
import time
from collections import deque
from queue import Full, Queue
from typing import Any


class EventBus:
    """In-process pub/sub for live task updates (SSE)."""

    def __init__(self, history: int = 200):
        self._lock = threading.Lock()
        self._subs: list[Queue] = []
        self._history: deque[dict[str, Any]] = deque(maxlen=history)
        self._snapshot: dict[str, Any] = {"tasks": [], "stats": {}, "ts": 0}
        self._seq = 0

    def publish(self, event: dict[str, Any]) -> None:
        event = dict(event)
        event.setdefault("ts", time.time())
        with self._lock:
            self._seq += 1
            event["seq"] = self._seq
            self._history.append(event)
            subs = list(self._subs)
        for q in subs:
            try:
                q.put_nowait(event)
            except Full:
                try:
                    q.get_nowait()
                except Exception:
                    pass
                try:
                    q.put_nowait(event)
                except Full:
                    pass

    def set_snapshot(self, snapshot: dict[str, Any]) -> None:
        snapshot = dict(snapshot)
        snapshot["ts"] = time.time()
        with self._lock:
            self._snapshot = snapshot
        self.publish({"type": "snapshot", **snapshot})

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._snapshot)

    def subscribe(self) -> Queue:
        q: Queue = Queue(maxsize=256)
        with self._lock:
            self._subs.append(q)
            snap = dict(self._snapshot)
            history = list(self._history)
        q.put({"type": "snapshot", **snap, "ts": time.time()})
        for event in history[-20:]:
            if event.get("type") != "snapshot":
                try:
                    q.put_nowait(event)
                except Full:
                    break
        return q

    def unsubscribe(self, q: Queue) -> None:
        with self._lock:
            if q in self._subs:
                self._subs.remove(q)
