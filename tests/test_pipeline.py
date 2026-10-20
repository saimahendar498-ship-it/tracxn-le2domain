from app.db import utcnow
from app.pipeline import Waiting
from app.worker import WorkerPool
from tests.conftest import FakeDFS, FakeLLM


def _release_waiting(harness):
    harness["db"].execute(
        "UPDATE tasks SET run_after = ? WHERE status = 'waiting'",
        [utcnow()],
    )


def drain(harness, limit: int = 40):
    pool = WorkerPool(
        harness["queue"], harness["pipeline"], worker_count=0, stale_seconds=60
    )
    ran = 0
    for _ in range(limit):
        if pool.run_once("test-worker"):
            ran += 1
            continue
        waiting = harness["queue"].list_tasks(status="waiting")
        if waiting:
            _release_waiting(harness)
            continue
        break
    return ran


def test_failed_domains_are_retried(harness):
    pipeline = harness["pipeline"]
    now = __import__("app.db", fromlist=["utcnow"]).utcnow()
    pipeline.db.execute(
        """
        INSERT INTO domains (domain, input_raw, pipeline_status, current_stage, error, created_at, updated_at)
        VALUES ('retry.in', 'retry.in', 'failed', 'extract_1', 'boom', ?, ?)
        """,
        [now, now],
    )
    pipeline.db.execute(
        "INSERT INTO domain_results (domain, updated_at) VALUES ('retry.in', ?)",
        [now],
    )
    assert pipeline.retry_failed() == 1
    row = pipeline.get_domain("retry.in")
    assert row["pipeline_status"] == "queued"
    removed = pipeline.delete_domain("retry.in")
    assert removed["deleted"] is True
    assert pipeline.db.fetchone("SELECT 1 FROM domains WHERE domain = 'retry.in'") is None


def test_full_pipeline_found(harness):
    pipeline = harness["pipeline"]
    pipeline.submit(["acme.in"])
    drain(harness)
    row = pipeline.get_domain("acme.in")
    assert row["pipeline_status"] == "completed"
    assert row["result"]["status_1"] == "FOUND"
    assert row["result"]["outcome"] == "ENTITY_FOUND"
    assert harness["llm"].extract_calls == 1
    assert harness["llm"].validate_calls == 1
    assert len(harness["dfs"].posts) == 2  # identify + validate; skip retry


def test_retry_pass_when_not_found(harness):
    harness["pipeline"].llm = FakeLLM(status="NOT_FOUND")
    harness["llm"] = harness["pipeline"].llm
    harness["pipeline"].submit(["missing.in"])
    drain(harness, limit=60)
    row = harness["pipeline"].get_domain("missing.in")
    assert row["pipeline_status"] == "completed"
    assert harness["llm"].extract_calls == 2
    assert len(harness["dfs"].posts) == 3


def test_serp_wait_then_complete(harness):
    harness["pipeline"].dfs = FakeDFS(pending_times=2)
    harness["dfs"] = harness["pipeline"].dfs
    harness["pipeline"].submit(["wait.example"])
    pool = WorkerPool(harness["queue"], harness["pipeline"], worker_count=0)
    statuses = []
    for _ in range(30):
        if not pool.run_once("w"):
            waiting = harness["queue"].list_tasks(status="waiting")
            if waiting:
                statuses.append("waiting")
                _release_waiting(harness)
                continue
            break
        statuses.append("ran")
    row = harness["pipeline"].get_domain("wait.example")
    assert row["pipeline_status"] == "completed"
    assert "waiting" in statuses


def test_waiting_exception_from_poll(harness):
    harness["pipeline"].dfs = FakeDFS(pending_times=99)
    harness["pipeline"].submit(["hold.example"])
    pool = WorkerPool(harness["queue"], harness["pipeline"], worker_count=0)
    for _ in range(6):
        if not pool.run_once("w"):
            break
    waiting = harness["queue"].list_tasks(status="waiting", domain="hold.example")
    assert waiting
    assert waiting[0]["stage"] == "serp_poll_1"
    row = harness["pipeline"]._result_row("hold.example")
    try:
        harness["pipeline"]._serp_poll("hold.example", row, True, 1, {"poll_attempts": 0})
        raise AssertionError("expected Waiting")
    except Waiting as pending:
        assert pending.seconds > 0
