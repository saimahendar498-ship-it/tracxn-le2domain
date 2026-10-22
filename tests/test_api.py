import httpx

from app.api import create_app
from app.config import Settings
from app.db import Database
from tests.conftest import FakeDFS, FakeLLM


def _client(app):
    return httpx.Client(transport=httpx.WSGITransport(app=app), base_url="http://test")


def test_api_submit_get_and_dedup():
    settings = Settings(
        duckdb_path=":memory:",
        worker_count=0,
        nvidia_api_keys=["k"],
        dfs_login="u",
        dfs_password="p",
    )
    db = Database(":memory:")
    app = create_app(settings=settings, db=db, start_workers=False)
    app.ctx.pipeline.dfs = FakeDFS()
    app.ctx.pipeline.llm = FakeLLM()
    with _client(app) as client:
        res = client.post("/api/v1/domains", json={"domains": ["https://www.Acme.in"]})
        assert res.status_code == 200
        item = res.json()["items"][0]
        assert item["domain"] == "acme.in"
        assert item["action"] == "queued"
        assert "in line" in item["message"].lower()
        again = client.post("/api/v1/domains", json={"domains": ["acme.in"]})
        assert again.json()["items"][0]["action"] == "in_progress"
        got = client.get("/api/v1/domains/acme.in")
        assert got.status_code == 200
        client.post("/api/v1/domains", json={"domains": ["example.com"]})
        db.execute(
            "UPDATE domain_results SET status_final = ? WHERE domain = ?",
            ["VALID", "acme.in"],
        )
        task_id = got.json()["active_task_id"]
        many = client.post(
            "/api/v1/domains/lookup",
            json={"domains": ["acme.in", "example.com", "missing.com"], "ids": [task_id, "no-such-id"]},
        )
        assert many.status_code == 200
        by_key = {}
        for row in many.json()["items"]:
            by_key[row.get("id") or row["domain"]] = row
        assert by_key["acme.in"]["found"] is True
        assert "status_final" not in (by_key["acme.in"]["result"] or {})
        page = client.get("/domain/acme.in")
        assert "const hideStatusFinal = true" in page.text
        assert by_key["example.com"]["found"] is True
        assert by_key["missing.com"]["found"] is False
        assert by_key[task_id]["found"] is True
        assert by_key[task_id]["domain"] == "acme.in"
        assert by_key["no-such-id"]["found"] is False
        empty = client.post("/api/v1/domains/lookup", json={"domains": [], "ids": []})
        assert empty.status_code == 400
        health = client.get("/api/v1/health")
        assert health.json()["ok"] is True
        bad = client.post("/api/v1/domains", json={"domains": ["not a domain"]})
        assert bad.status_code == 400
        removed = client.delete("/api/v1/domains/acme.in")
        assert removed.status_code == 200
        assert removed.json()["deleted"] is True
        missing = client.get("/api/v1/domains/acme.in")
        assert missing.status_code == 404
        gone = client.delete("/api/v1/domains/acme.in")
        assert gone.status_code == 404
    db.close()


def test_status_final_can_be_shown():
    settings = Settings(
        duckdb_path=":memory:",
        worker_count=0,
        nvidia_api_keys=["k"],
        dfs_login="u",
        dfs_password="p",
        hide_status_final=False,
    )
    db = Database(":memory:")
    app = create_app(settings=settings, db=db, start_workers=False)
    app.ctx.pipeline.dfs = FakeDFS()
    app.ctx.pipeline.llm = FakeLLM()
    with _client(app) as client:
        client.post("/api/v1/domains", json={"domains": ["acme.in"]})
        db.execute(
            "UPDATE domain_results SET status_final = ? WHERE domain = ?",
            ["VALID", "acme.in"],
        )
        many = client.post("/api/v1/domains/lookup", json={"domains": ["acme.in"]})
        result = many.json()["items"][0]["result"]
        assert result["status_final"] == "VALID"
        page = client.get("/domain/acme.in")
        assert "const hideStatusFinal = false" in page.text
    db.close()


def test_api_schedule_and_tasks():
    settings = Settings(duckdb_path=":memory:", worker_count=0, nvidia_api_keys=["k"])
    db = Database(":memory:")
    app = create_app(settings=settings, db=db, start_workers=False)
    with _client(app) as client:
        created = client.post(
            "/api/v1/schedules",
            json={"name": "hourly", "job_type": "reprocess_pending", "cron_expr": "0 * * * *"},
        )
        assert created.status_code == 200
        listed = client.get("/api/v1/schedules")
        assert listed.json()["items"]
        tasks = client.get("/api/v1/tasks")
        assert tasks.status_code == 200
        page = client.get("/")
        assert page.status_code == 200
        assert "Look up a website" in page.text
        domains = client.get("/domains")
        assert domains.status_code == 200
        assert "Websites" in domains.text
        assert "setInterval(tick, 30)" in domains.text
        one = client.get("/domain/example.com")
        assert one.status_code == 200
        assert "Company" in one.text
    db.close()
