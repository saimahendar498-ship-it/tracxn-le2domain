from datetime import datetime

from app.cron import cron_matches, next_cron
from app.db import utcnow
from app.scheduler import Scheduler


def test_cron_hourly():
    dt = datetime(2026, 10, 20, 11, 0)
    assert cron_matches("0 * * * *", dt)
    assert not cron_matches("0 * * * *", dt.replace(minute=1))
    nxt = next_cron("0 * * * *", dt)
    assert nxt.hour == 12 and nxt.minute == 0


def test_scheduler_interval_enqueue(harness):
    sched = Scheduler(harness["db"], harness["queue"], harness["pipeline"], tick_seconds=0.01)
    created = sched.create(
        "pending-pass",
        "reprocess_pending",
        interval_seconds=60,
        enabled=True,
    )
    harness["db"].execute(
        "UPDATE schedules SET next_run_at = ? WHERE id = ?",
        [utcnow(), created["id"]],
    )
    fired = sched.tick()
    assert fired == 1
    tasks = harness["queue"].list_tasks()
    assert any(t["job_type"] == "reprocess_pending" for t in tasks)
    updated = sched.get(created["id"])
    assert updated["last_run_at"] is not None
