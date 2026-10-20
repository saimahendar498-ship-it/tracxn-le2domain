from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

from app.db import utcnow
from app.worker import WorkerPool


def test_retry_then_fail(harness):
    queue = harness["queue"]

    def boom(task):
        raise RuntimeError("boom")

    harness["pipeline"].run_task = boom
    task = queue.enqueue("pipeline_stage", domain="x.test", stage="identify_prompt", max_attempts=2)
    pool = WorkerPool(queue, harness["pipeline"], worker_count=0)
    pool.run_once("w1")
    again = queue.get(task["id"])
    assert again["status"] == "queued"
    assert again["attempts"] == 1
    queue.db.execute("UPDATE tasks SET run_after = ? WHERE id = ?", [utcnow(), task["id"]])
    pool.run_once("w2")
    failed = queue.get(task["id"])
    assert failed["status"] == "failed"


def test_recovery_of_stale_running(harness):
    queue = harness["queue"]
    task = queue.enqueue("pipeline_stage", domain="stale.test", stage="identify_prompt")
    stale = utcnow() - timedelta(seconds=400)
    harness["db"].execute(
        """
        UPDATE tasks
        SET status = 'running', locked_at = ?, locked_by = 'dead-worker'
        WHERE id = ?
        """,
        [stale, task["id"]],
    )
    count = queue.recover_stale(300)
    assert count == 1
    recovered = queue.get(task["id"])
    assert recovered["status"] == "queued"
    assert recovered["locked_by"] is None


def test_event_ids_do_not_collide(harness):
    def emit(i):
        harness["queue"]._emit({"id": f"t{i}", "domain": "a.com"}, "tick")

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(emit, range(40)))
    rows = harness["db"].fetchall("SELECT id FROM events")
    ids = [row[0] for row in rows]
    assert len(ids) == 40
    assert len(set(ids)) == 40


def test_same_step_is_not_queued_twice(harness):
    first = harness["queue"].enqueue(
        "pipeline_stage", domain="acme.in", stage="extract_1"
    )
    second = harness["queue"].enqueue(
        "pipeline_stage", domain="acme.in", stage="extract_1"
    )
    assert second["id"] == first["id"]
    count = harness["db"].fetchone(
        "SELECT COUNT(*) FROM tasks WHERE domain = 'acme.in' AND stage = 'extract_1'"
    )
    assert count[0] == 1
    harness["db"].execute(
        """
        INSERT INTO tasks (
            id, job_type, domain, stage, status, priority, attempts, max_attempts,
            run_after, payload, created_at, updated_at
        ) VALUES ('extra', 'pipeline_stage', 'acme.in', 'extract_1', 'queued', 1, 0, 5, ?, '{}', ?, ?)
        """,
        [utcnow(), utcnow(), utcnow()],
    )
    removed = harness["queue"].cancel_duplicate_tasks()
    assert removed == 1
    left = harness["db"].fetchone(
        """
        SELECT COUNT(*) FROM tasks
        WHERE domain = 'acme.in' AND stage = 'extract_1' AND status = 'queued'
        """
    )
    assert left[0] == 1


def test_auth_failure_is_retried(harness):
    queue = harness["queue"]

    def denied(task):
        raise RuntimeError("Error code: 403 - Authorization failed")

    harness["pipeline"].run_task = denied
    task = queue.enqueue(
        "pipeline_stage", domain="auth.test", stage="extract_1", max_attempts=5
    )
    WorkerPool(queue, harness["pipeline"], worker_count=0).run_once("w")
    again = queue.get(task["id"])
    assert again["status"] == "queued"
    assert again["attempts"] == 1


def test_priority_claim_order(harness):
    queue = harness["queue"]
    low = queue.enqueue("pipeline_stage", domain="low.test", stage="identify_prompt", priority=1)
    high = queue.enqueue("pipeline_stage", domain="high.test", stage="identify_prompt", priority=500)
    claimed = queue.claim("w")
    assert claimed["id"] == high["id"]
    assert claimed["id"] != low["id"]
