from __future__ import annotations

import logging
import uuid
from datetime import timedelta
from typing import Any

log = logging.getLogger("anid2le.queue")

from app.db import Database, dumps, loads, utcnow
from app.events import EventBus

ACTIVE_DOMAIN_STATUSES = ("queued", "running", "waiting")
ACTIVE_TASK_STATUSES = ("queued", "running", "waiting")


class TaskQueue:
    def __init__(self, db: Database, bus: EventBus):
        self.db = db
        self.bus = bus

    def enqueue(
        self,
        job_type: str,
        *,
        domain: str | None = None,
        stage: str | None = None,
        payload: dict[str, Any] | None = None,
        priority: int = 100,
        delay_seconds: float = 0,
        max_attempts: int = 5,
        task_id: str | None = None,
        emit: bool = True,
    ) -> dict[str, Any]:
        existing = self._active_task(domain, stage)
        if existing and not task_id:
            return existing
        now = utcnow()
        run_after = now + timedelta(seconds=delay_seconds)
        task_id = task_id or str(uuid.uuid4())
        self.db.execute(
            """
            INSERT INTO tasks (
                id, job_type, domain, stage, status, priority, attempts, max_attempts,
                run_after, payload, created_at, updated_at
            ) VALUES (?, ?, ?, ?, 'queued', ?, 0, ?, ?, ?, ?, ?)
            """,
            [
                task_id,
                job_type,
                domain,
                stage,
                priority,
                max_attempts,
                run_after,
                dumps(payload or {}),
                now,
                now,
            ],
        )
        task = self.get(task_id)
        if emit:
            self._emit(task, "enqueued")
        return task

    def enqueue_many(self, specs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not specs:
            return []
        now = utcnow()
        rows = []
        shaped = []
        for spec in specs:
            existing = self._active_task(spec.get("domain"), spec.get("stage"))
            if existing:
                shaped.append(existing)
                continue
            task_id = spec.get("task_id") or str(uuid.uuid4())
            payload = spec.get("payload") or {}
            delay = float(spec.get("delay_seconds") or 0)
            run_after = now + timedelta(seconds=delay)
            item = {
                "id": task_id,
                "job_type": spec["job_type"],
                "domain": spec.get("domain"),
                "stage": spec.get("stage"),
                "status": "queued",
                "priority": spec.get("priority", 100),
                "attempts": 0,
                "max_attempts": spec.get("max_attempts", 5),
                "run_after": run_after.isoformat(),
                "payload": payload,
                "result": None,
                "error": None,
                "created_at": now.isoformat(),
                "updated_at": now.isoformat(),
                "locked_at": None,
                "locked_by": None,
                "started_at": None,
                "finished_at": None,
            }
            rows.append(
                (
                    task_id,
                    item["job_type"],
                    item["domain"],
                    item["stage"],
                    item["priority"],
                    item["max_attempts"],
                    run_after,
                    dumps(payload),
                    now,
                    now,
                )
            )
            shaped.append(item)
        self.db.executemany(
            """
            INSERT INTO tasks (
                id, job_type, domain, stage, status, priority, attempts, max_attempts,
                run_after, payload, created_at, updated_at
            ) VALUES (?, ?, ?, ?, 'queued', ?, 0, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        return shaped

    def get(self, task_id: str) -> dict[str, Any] | None:
        row = self.db.fetch_dict("SELECT * FROM tasks WHERE id = ?", [task_id])
        return self._shape(row) if row else None

    def list_tasks(
        self,
        status: str | None = None,
        domain: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM tasks"
        clauses = []
        params: list[Any] = []
        if status:
            clauses.append("status = ?")
            params.append(status)
        if domain:
            clauses.append("domain = ?")
            params.append(domain)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY updated_at DESC LIMIT ?"
        params.append(limit)
        return [self._shape(r) for r in self.db.fetch_dicts(sql, params)]

    def snapshot(self) -> dict[str, Any]:
        tasks = self.list_tasks(limit=150)
        stats_rows = self.db.fetch_dicts(
            "SELECT status, COUNT(*) AS n FROM tasks GROUP BY status"
        )
        stats = {r["status"]: int(r["n"]) for r in stats_rows}
        domain_rows = self.db.fetch_dicts(
            "SELECT pipeline_status, COUNT(*) AS n FROM domains GROUP BY pipeline_status"
        )
        return {
            "tasks": tasks,
            "stats": stats,
            "domains": {r["pipeline_status"]: int(r["n"]) for r in domain_rows},
        }

    def claim(self, worker_id: str) -> dict[str, Any] | None:
        now = utcnow()
        with self.db.cursor() as con:
            row = con.execute(
                """
                SELECT id FROM tasks
                WHERE status IN ('queued', 'waiting') AND run_after <= ?
                ORDER BY priority DESC, created_at ASC
                LIMIT 1
                """,
                [now],
            ).fetchone()
            if not row:
                return None
            task_id = row[0]
            con.execute(
                """
                UPDATE tasks
                SET status = 'running',
                    locked_at = ?,
                    locked_by = ?,
                    started_at = COALESCE(started_at, ?),
                    updated_at = ?,
                    attempts = attempts + 1
                WHERE id = ? AND status IN ('queued', 'waiting')
                """,
                [now, worker_id, now, now, task_id],
            )
        task = self.get(task_id)
        if task:
            try:
                self._emit(task, "running")
            except Exception:
                log.exception("could not publish task %s", task_id)
        return task

    def complete(self, task_id: str, result: dict[str, Any] | None = None) -> None:
        now = utcnow()
        self.db.execute(
            """
            UPDATE tasks
            SET status = 'completed', result = ?, error = NULL,
                finished_at = ?, updated_at = ?, locked_at = NULL, locked_by = NULL
            WHERE id = ?
            """,
            [dumps(result), now, now, task_id],
        )
        task = self.get(task_id)
        if task:
            self._emit(task, "completed")

    def fail(self, task_id: str, error: str, retry: bool = True, delay_seconds: float = 0) -> None:
        task = self.get(task_id)
        if not task:
            return
        now = utcnow()
        if retry and task["attempts"] < task["max_attempts"]:
            run_after = now + timedelta(seconds=delay_seconds or min(2 ** task["attempts"], 60))
            self.db.execute(
                """
                UPDATE tasks
                SET status = 'queued', error = ?, run_after = ?,
                    updated_at = ?, locked_at = NULL, locked_by = NULL
                WHERE id = ?
                """,
                [error, run_after, now, task_id],
            )
            task = self.get(task_id)
            if task:
                self._emit(task, "retry")
            return
        self.db.execute(
            """
            UPDATE tasks
            SET status = 'failed', error = ?, finished_at = ?, updated_at = ?,
                locked_at = NULL, locked_by = NULL
            WHERE id = ?
            """,
            [error, now, now, task_id],
        )
        task = self.get(task_id)
        if task:
            self._emit(task, "failed")

    def update_payload(self, task_id: str, payload: dict[str, Any]) -> None:
        self.db.execute(
            "UPDATE tasks SET payload = ?, updated_at = ? WHERE id = ?",
            [dumps(payload), utcnow(), task_id],
        )

    def wait(self, task_id: str, delay_seconds: float, note: str | None = None) -> None:
        now = utcnow()
        run_after = now + timedelta(seconds=delay_seconds)
        self.db.execute(
            """
            UPDATE tasks
            SET status = 'waiting', run_after = ?, error = ?, updated_at = ?,
                locked_at = NULL, locked_by = NULL, attempts = attempts - 1
            WHERE id = ?
            """,
            [run_after, note, now, task_id],
        )
        task = self.get(task_id)
        if task:
            self._emit(task, "waiting")

    def cancel(self, task_id: str) -> dict[str, Any] | None:
        now = utcnow()
        self.db.execute(
            """
            UPDATE tasks
            SET status = 'cancelled', updated_at = ?, finished_at = ?,
                locked_at = NULL, locked_by = NULL
            WHERE id = ? AND status IN ('queued', 'waiting', 'running')
            """,
            [now, now, task_id],
        )
        task = self.get(task_id)
        if task:
            self._emit(task, "cancelled")
        return task

    def _active_task(self, domain: str | None, stage: str | None) -> dict[str, Any] | None:
        if not domain or not stage:
            return None
        row = self.db.fetchone(
            """
            SELECT id FROM tasks
            WHERE domain = ? AND stage = ? AND status IN ('queued', 'running', 'waiting')
            ORDER BY created_at
            LIMIT 1
            """,
            [domain, stage],
        )
        return self.get(row[0]) if row else None

    def cancel_duplicate_tasks(self) -> int:
        now = utcnow()
        before = self.db.fetchone(
            """
            SELECT COUNT(*) FROM tasks
            WHERE status IN ('queued', 'running', 'waiting')
              AND domain IS NOT NULL AND stage IS NOT NULL
            """
        )
        self.db.execute(
            """
            UPDATE tasks
            SET status = 'cancelled', updated_at = ?, finished_at = ?,
                locked_at = NULL, locked_by = NULL,
                error = 'duplicate step removed'
            WHERE status IN ('queued', 'running', 'waiting')
              AND domain IS NOT NULL AND stage IS NOT NULL
              AND id NOT IN (
                SELECT id FROM (
                    SELECT id, ROW_NUMBER() OVER (
                        PARTITION BY domain, stage ORDER BY created_at
                    ) AS rn
                    FROM tasks
                    WHERE status IN ('queued', 'running', 'waiting')
                      AND domain IS NOT NULL AND stage IS NOT NULL
                ) ranked
                WHERE rn = 1
              )
            """,
            [now, now],
        )
        after = self.db.fetchone(
            """
            SELECT COUNT(*) FROM tasks
            WHERE status IN ('queued', 'running', 'waiting')
              AND domain IS NOT NULL AND stage IS NOT NULL
            """
        )
        return int((before[0] if before else 0) - (after[0] if after else 0))

    def recover_running(self) -> int:
        """On startup nothing is really running. Put those jobs back in line."""
        before = self.db.fetchone(
            "SELECT COUNT(*) FROM tasks WHERE status = 'running'"
        )
        self.db.execute(
            """
            UPDATE tasks
            SET status = 'queued', locked_at = NULL, locked_by = NULL, updated_at = ?,
                run_after = ?, error = NULL
            WHERE status = 'running'
            """,
            [utcnow(), utcnow()],
        )
        return int(before[0] if before else 0)

    def recover_stale(self, stale_seconds: int) -> int:
        cutoff = utcnow() - timedelta(seconds=stale_seconds)
        before = self.db.fetchone(
            "SELECT COUNT(*) FROM tasks WHERE status = 'running' AND locked_at IS NOT NULL AND locked_at < ?",
            [cutoff],
        )
        self.db.execute(
            """
            UPDATE tasks
            SET status = 'queued', locked_at = NULL, locked_by = NULL, updated_at = ?,
                error = 'recovered after worker restart'
            WHERE status = 'running' AND locked_at IS NOT NULL AND locked_at < ?
            """,
            [utcnow(), cutoff],
        )
        count = int(before[0] if before else 0)
        if count:
            self.bus.publish({"type": "recovery", "count": count})
        return count

    def _shape(self, row: dict[str, Any] | None) -> dict[str, Any] | None:
        if not row:
            return None
        out = dict(row)
        out["payload"] = loads(out.get("payload"), {})
        out["result"] = loads(out.get("result"), None)
        for key in (
            "created_at",
            "updated_at",
            "run_after",
            "locked_at",
            "started_at",
            "finished_at",
        ):
            val = out.get(key)
            if hasattr(val, "isoformat"):
                out[key] = val.isoformat()
        return out

    def _emit(self, task: dict[str, Any], event_type: str) -> None:
        now = utcnow()
        try:
            self.db.execute(
                """
                INSERT INTO events (id, task_id, domain, event_type, payload, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    str(uuid.uuid4()),
                    task.get("id"),
                    task.get("domain"),
                    event_type,
                    dumps(task),
                    now,
                ],
            )
        except Exception:
            log.exception("could not store task event")
        self.bus.publish({"type": "task", "event": event_type, "task": task})
        try:
            self.bus.set_snapshot(self.snapshot())
        except Exception:
            log.exception("could not refresh the live list")
