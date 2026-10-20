from __future__ import annotations

import atexit
import json
import logging
from pathlib import Path
from queue import Empty
from typing import Any

from bottle import Bottle, HTTPError, abort, request, response, static_file

from app.config import Settings, get_settings
from app.db import Database
from app.domainutil import normalize_domain
from app.events import EventBus
from app.pipeline import Pipeline
from app.providers import DataForSEOClient, LLMClient
from app.queue import TaskQueue
from app.scheduler import Scheduler
from app.worker import WorkerPool

log = logging.getLogger("anid2le.api")
STATIC_DIR = Path(__file__).resolve().parent / "static"


class AppContext:
    def __init__(self, settings: Settings, db: Database | None = None, start_workers: bool = True):
        self.settings = settings
        self.db = db or Database(settings.duckdb_path)
        self.bus = EventBus()
        self.queue = TaskQueue(self.db, self.bus)
        self.dfs = DataForSEOClient(settings, self.db)
        self.llm = LLMClient(settings)
        self.pipeline = Pipeline(
            self.db,
            self.queue,
            self.dfs,
            self.llm,
            poll_delay=settings.dfs_poll_delay_sec,
            poll_max_attempts=settings.dfs_poll_max_attempts,
        )
        self.scheduler = Scheduler(
            self.db, self.queue, self.pipeline, tick_seconds=settings.scheduler_tick_sec
        )
        self.workers = WorkerPool(
            self.queue,
            self.pipeline,
            worker_count=settings.worker_count,
            stale_seconds=settings.task_stale_seconds,
        )
        self.bus.set_snapshot(self.queue.snapshot())
        if start_workers:
            self.workers.start()
            self.scheduler.start()
            atexit.register(self.shutdown)

    def shutdown(self) -> None:
        self.workers.stop()
        self.scheduler.stop()


def _json(data: Any, status: int = 200) -> str:
    response.status = status
    response.content_type = "application/json"
    return json.dumps(data, default=str)


def _body() -> dict[str, Any]:
    raw = request.body.read()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        abort(400, "Send JSON in the request body.")
    if not isinstance(data, dict):
        abort(400, "The request body must be a JSON object.")
    return data


def _limit(default: int = 200) -> int:
    raw = request.query.get("limit", str(default))
    try:
        value = int(raw)
    except ValueError:
        abort(400, "limit must be a number.")
    if value < 1 or value > 2000:
        abort(400, "limit must be between 1 and 2000.")
    return value


def create_app(
    settings: Settings | None = None,
    db: Database | None = None,
    start_workers: bool = True,
) -> Bottle:
    settings = settings or get_settings()
    logging.basicConfig(
        level=getattr(logging, settings.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    ctx = AppContext(settings, db=db, start_workers=start_workers)
    app = Bottle()
    app.ctx = ctx

    def as_json_error(error: HTTPError) -> str:
        response.content_type = "application/json"
        detail = error.body
        if isinstance(detail, bytes):
            detail = detail.decode()
        return json.dumps({"detail": str(detail)})

    for code in (400, 404, 409, 500):
        app.error(code)(as_json_error)

    @app.get("/api/v1/health")
    def health():
        return _json({"ok": True, "version": "1.0.0"})

    @app.post("/api/v1/domains")
    def submit_domains():
        body = _body()
        domains = body.get("domains") or []
        if not isinstance(domains, list) or not domains:
            abort(400, "Add at least one website.")
        try:
            items = ctx.pipeline.submit(
                [str(item) for item in domains],
                force=bool(body.get("force")),
                priority=int(body.get("priority") or 100),
                use_cache=body.get("use_cache", True) is not False,
            )
        except ValueError as exc:
            abort(400, str(exc))
        return _json({"items": items})

    @app.post("/api/v1/domains/lookup")
    def lookup_domains():
        body = _body()
        domains = body.get("domains") or []
        ids = body.get("ids") or []
        if isinstance(domains, str):
            domains = [domains]
        if isinstance(ids, str):
            ids = [ids]
        if not isinstance(domains, list) or not isinstance(ids, list):
            abort(400, "domains and ids must be lists.")
        try:
            items = ctx.pipeline.get_many(domains, ids)
        except ValueError as exc:
            abort(400, str(exc))
        return _json({"items": items})

    @app.get("/api/v1/domains")
    def list_domains():
        return _json({"items": ctx.pipeline.list_domains(limit=_limit())})

    @app.delete("/api/v1/domains/<domain>")
    def delete_domain(domain):
        try:
            return _json(ctx.pipeline.delete_domain(domain))
        except ValueError as exc:
            abort(400, str(exc))
        except KeyError:
            abort(404, "That website is not in the list yet.")

    @app.get("/api/v1/domains/<domain>")
    def get_domain(domain):
        try:
            return _json(ctx.pipeline.get_domain(domain))
        except ValueError as exc:
            abort(400, str(exc))
        except KeyError:
            abort(404, "That website is not in the list yet.")

    @app.get("/api/v1/results/<domain>")
    def get_result(domain):
        try:
            payload = ctx.pipeline.get_domain(domain)
            key = normalize_domain(domain)
        except ValueError as exc:
            abort(400, str(exc))
        except KeyError:
            abort(404, "That website is not in the list yet.")
        payload["full_result"] = ctx.db.fetch_dict(
            "SELECT * FROM domain_results WHERE domain = ?",
            [key],
        )
        return _json(payload)

    @app.get("/api/v1/tasks")
    def list_tasks():
        return _json(
            {
                "items": ctx.queue.list_tasks(
                    status=request.query.get("status") or None,
                    domain=request.query.get("domain") or None,
                    limit=_limit(),
                )
            }
        )

    @app.get("/api/v1/tasks/<task_id>")
    def get_task(task_id):
        task = ctx.queue.get(task_id)
        if not task:
            abort(404, "That task was not found.")
        return _json(task)

    @app.post("/api/v1/tasks/<task_id>/cancel")
    def cancel_task(task_id):
        task = ctx.queue.cancel(task_id)
        if not task:
            abort(404, "That task was not found.")
        return _json(task)

    @app.post("/api/v1/tasks/<task_id>/retry")
    def retry_task(task_id):
        task = ctx.queue.get(task_id)
        if not task:
            abort(404, "That task was not found.")
        if task["status"] not in ("failed", "cancelled"):
            abort(409, "Only a failed or cancelled task can be tried again.")
        nxt = ctx.queue.enqueue(
            task["job_type"],
            domain=task.get("domain"),
            stage=task.get("stage"),
            payload=task.get("payload") or {},
            priority=task.get("priority") or 100,
        )
        return _json(nxt)

    @app.get("/api/v1/live/snapshot")
    def live_snapshot():
        return _json(ctx.bus.snapshot())

    @app.get("/api/v1/live/events")
    def live_events():
        response.content_type = "text/event-stream"
        response.set_header("Cache-Control", "no-cache")
        response.set_header("X-Accel-Buffering", "no")
        bus = ctx.bus
        inbox = bus.subscribe()

        def gen():
            try:
                while True:
                    try:
                        event = inbox.get(timeout=15)
                    except Empty:
                        yield "event: ping\ndata: {}\n\n"
                        continue
                    yield f"data: {json.dumps(event, default=str)}\n\n"
            finally:
                bus.unsubscribe(inbox)

        return gen()

    @app.post("/api/v1/schedules")
    def create_schedule():
        body = _body()
        name = (body.get("name") or "").strip()
        if not name:
            abort(400, "Give the schedule a name.")
        try:
            created = ctx.scheduler.create(
                name,
                body.get("job_type") or "reprocess_pending",
                cron_expr=body.get("cron_expr") or None,
                interval_seconds=body.get("interval_seconds"),
                payload=body.get("payload") or {},
                enabled=body.get("enabled", True) is not False,
            )
        except ValueError as exc:
            abort(400, str(exc))
        return _json(created)

    @app.get("/api/v1/schedules")
    def list_schedules():
        return _json({"items": ctx.scheduler.list()})

    @app.post("/api/v1/schedules/<schedule_id>/enable")
    def enable_schedule(schedule_id):
        row = ctx.scheduler.set_enabled(schedule_id, True)
        if not row:
            abort(404, "That schedule was not found.")
        return _json(row)

    @app.post("/api/v1/schedules/<schedule_id>/disable")
    def disable_schedule(schedule_id):
        row = ctx.scheduler.set_enabled(schedule_id, False)
        if not row:
            abort(404, "That schedule was not found.")
        return _json(row)

    @app.get("/")
    def index():
        return static_file("index.html", root=str(STATIC_DIR))

    @app.get("/domains")
    def domains_page():
        return static_file("domains.html", root=str(STATIC_DIR))

    @app.get("/domain/<domain>")
    def domain_page(domain):
        return static_file("domain.html", root=str(STATIC_DIR))

    return app
