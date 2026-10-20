from __future__ import annotations

import logging
import threading
import time
import uuid
from datetime import timedelta
from typing import Any

from app.cron import next_cron
from app.db import Database, dumps, loads, utcnow
from app.pipeline import Pipeline
from app.queue import TaskQueue

log = logging.getLogger("anid2le.scheduler")


class Scheduler:
    def __init__(
        self,
        db: Database,
        queue: TaskQueue,
        pipeline: Pipeline,
        tick_seconds: float = 5.0,
    ):
        self.db = db
        self.queue = queue
        self.pipeline = pipeline
        self.tick_seconds = tick_seconds
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="scheduler", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None

    def create(
        self,
        name: str,
        job_type: str,
        *,
        cron_expr: str | None = None,
        interval_seconds: int | None = None,
        payload: dict[str, Any] | None = None,
        enabled: bool = True,
    ) -> dict[str, Any]:
        if not cron_expr and not interval_seconds:
            raise ValueError("cron_expr or interval_seconds is required")
        now = utcnow()
        schedule_id = str(uuid.uuid4())
        next_run = self._compute_next(cron_expr, interval_seconds, now)
        self.db.execute(
            """
            INSERT INTO schedules (
                id, name, cron_expr, interval_seconds, job_type, payload,
                enabled, next_run_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                schedule_id,
                name,
                cron_expr,
                interval_seconds,
                job_type,
                dumps(payload or {}),
                enabled,
                next_run,
                now,
                now,
            ],
        )
        return self.get(schedule_id)

    def get(self, schedule_id: str) -> dict[str, Any] | None:
        row = self.db.fetch_dict("SELECT * FROM schedules WHERE id = ?", [schedule_id])
        return self._shape(row)

    def list(self) -> list[dict[str, Any]]:
        return [self._shape(r) for r in self.db.fetch_dicts("SELECT * FROM schedules ORDER BY name")]

    def set_enabled(self, schedule_id: str, enabled: bool) -> dict[str, Any] | None:
        self.db.execute(
            "UPDATE schedules SET enabled = ?, updated_at = ? WHERE id = ?",
            [enabled, utcnow(), schedule_id],
        )
        return self.get(schedule_id)

    def tick(self) -> int:
        now = utcnow()
        due = self.db.fetch_dicts(
            """
            SELECT * FROM schedules
            WHERE enabled = TRUE AND next_run_at IS NOT NULL AND next_run_at <= ?
            """,
            [now],
        )
        fired = 0
        for row in due:
            self._fire(row, now)
            fired += 1
        return fired

    def _fire(self, row: dict[str, Any], now) -> None:
        payload = loads(row.get("payload"), {}) or {}
        job_type = row["job_type"]
        try:
            if job_type == "submit_domains":
                domains = payload.get("domains") or []
                self.pipeline.submit(
                    domains,
                    force=bool(payload.get("force")),
                    priority=int(payload.get("priority") or 100),
                )
            else:
                self.queue.enqueue(
                    job_type,
                    payload=payload,
                    priority=int(payload.get("priority") or 50),
                )
        except Exception:
            log.exception("schedule %s failed to enqueue", row.get("id"))
        nxt = self._compute_next(row.get("cron_expr"), row.get("interval_seconds"), now)
        self.db.execute(
            """
            UPDATE schedules
            SET last_run_at = ?, next_run_at = ?, updated_at = ?
            WHERE id = ?
            """,
            [now, nxt, now, row["id"]],
        )

    def _compute_next(
        self, cron_expr: str | None, interval_seconds: int | None, after
    ):
        if cron_expr:
            return next_cron(cron_expr, after)
        seconds = int(interval_seconds or 60)
        return after + timedelta(seconds=seconds)

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception:
                log.exception("scheduler tick failed")
            time.sleep(self.tick_seconds)

    def _shape(self, row: dict[str, Any] | None) -> dict[str, Any] | None:
        if not row:
            return None
        out = dict(row)
        out["payload"] = loads(out.get("payload"), {})
        for key in ("created_at", "updated_at", "last_run_at", "next_run_at"):
            val = out.get(key)
            if hasattr(val, "isoformat"):
                out[key] = val.isoformat()
        return out
