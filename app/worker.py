from __future__ import annotations

import logging
import threading
import time
import uuid

from app.db import utcnow
from app.pipeline import Pipeline, Waiting
from app.queue import TaskQueue

log = logging.getLogger("anid2le.worker")


class WorkerPool:
    def __init__(
        self,
        queue: TaskQueue,
        pipeline: Pipeline,
        *,
        worker_count: int = 4,
        stale_seconds: int = 300,
        idle_sleep: float = 0.2,
    ):
        self.queue = queue
        self.pipeline = pipeline
        self.worker_count = worker_count
        self.stale_seconds = stale_seconds
        self.idle_sleep = idle_sleep
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self._recover_lock = threading.Lock()
        self._last_recover = 0.0

    def start(self) -> None:
        recovered = self.queue.recover_running()
        self.queue.cancel_duplicate_tasks()
        retried = self.pipeline.retry_failed()
        self._last_recover = time.monotonic()
        if recovered:
            log.info("requeued %s tasks after restart", recovered)
        if retried:
            log.info("retrying %s failed websites", retried)
        self._stop.clear()
        for i in range(self.worker_count):
            thread = threading.Thread(
                target=self._loop, name=f"worker-{i}", daemon=True
            )
            thread.start()
            self._threads.append(thread)

    def stop(self) -> None:
        self._stop.set()
        for thread in self._threads:
            thread.join(timeout=5)
        self._threads.clear()

    def run_once(self, worker_id: str | None = None) -> bool:
        task = self.queue.claim(worker_id or f"once-{uuid.uuid4()}")
        if not task:
            return False
        self._execute(task)
        return True

    def _maybe_recover(self) -> None:
        now = time.monotonic()
        if now - self._last_recover < 30:
            return
        with self._recover_lock:
            if time.monotonic() - self._last_recover < 30:
                return
            self._last_recover = time.monotonic()
            self.queue.recover_stale(self.stale_seconds)
            self.pipeline.retry_failed(min_age_seconds=60)

    def _loop(self) -> None:
        worker_id = f"{threading.current_thread().name}-{uuid.uuid4().hex[:8]}"
        while not self._stop.is_set():
            self._maybe_recover()
            try:
                task = self.queue.claim(worker_id)
            except Exception:
                log.exception("worker %s failed while taking a job", worker_id)
                time.sleep(1)
                continue
            if not task:
                time.sleep(self.idle_sleep)
                continue
            self._execute(task)

    def _execute(self, task: dict) -> None:
        domain = task.get("domain")
        try:
            result = self.pipeline.run_task(task)
            self.queue.complete(task["id"], result)
        except Waiting as waiting:
            if domain:
                self.pipeline.db.execute(
                    """
                    UPDATE domains
                    SET pipeline_status = 'waiting', updated_at = ?
                    WHERE domain = ?
                    """,
                    [utcnow(), domain],
                )
            self.queue.update_payload(task["id"], task.get("payload") or {})
            self.queue.wait(task["id"], waiting.seconds, waiting.note)
        except Exception as exc:
            log.exception("task %s failed", task.get("id"))
            error = str(exc)
            try:
                self.queue.fail(task["id"], error, retry=True)
                failed = self.queue.get(task["id"])
            except Exception:
                log.exception("could not record failure for %s", task.get("id"))
                failed = None
            if domain and failed and failed["status"] == "failed":
                self.pipeline.fail_domain(domain, error)
