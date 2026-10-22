from __future__ import annotations

import pytest

from app.config import Settings
from app.db import Database
from app.events import EventBus
from app.pipeline import Pipeline
from app.queue import TaskQueue


class FakeDFS:
    def __init__(self, pending_times: int = 0):
        self.posts: list[str] = []
        self.polls = 0
        self.pending_times = pending_times

    def post_ai_mode(self, keyword: str, use_cache: bool = True):
        self.posts.append(keyword)
        return {"task_id": f"task-{len(self.posts)}", "cost": 0.0012}

    def get_ai_mode(self, task_id: str, use_cache: bool = True, login: str | None = None):
        self.polls += 1
        if self.polls <= self.pending_times:
            return {"pending": True, "status_code": 40601}
        return {
            "status_code": 20000,
            "task": {
                "result": [
                    {
                        "items": [
                            {
                                "text": "Acme Private Limited operates the domain.",
                                "markdown": "Acme Private Limited is a Private Limited company.",
                                "references": [
                                    {"title": "MCA", "url": "https://www.mca.gov.in"}
                                ],
                            }
                        ]
                    }
                ]
            },
        }


class FakeLLM:
    def __init__(self, status: str = "FOUND", entity_type: str = "Private Limited"):
        self.status = status
        self.entity_type = entity_type
        self.extract_calls = 0
        self.validate_calls = 0

    def extract_entity(self, text: str, seed: int = 0):
        self.extract_calls += 1
        name = "Acme Private Limited" if self.status == "FOUND" else ""
        return {
            "LE_NAME": name,
            "ENTITY_TYPE": self.entity_type if self.status == "FOUND" else "",
            "SOURCE_URLS": ["https://www.mca.gov.in"] if self.status == "FOUND" else [],
            "EVIDENCE": "from serp" if self.status == "FOUND" else "",
            "STATUS": self.status,
        }

    def extract_validation(self, text: str, seed: int = 0):
        self.validate_calls += 1
        return {
            "LE_NAME": "Acme Private Limited",
            "ENTITY_TYPE": "Private Limited",
            "SOURCE_URLS": ["https://www.mca.gov.in"],
            "EVIDENCE": "validated",
            "STATUS": "VALID",
            "OUTCOME": "ENTITY_FOUND",
        }


@pytest.fixture
def settings():
    return Settings(
        duckdb_path=":memory:",
        worker_count=1,
        dfs_poll_delay_sec=0.01,
        nvidia_api_keys=["test-key"],
        dfs_login="user",
        dfs_password="pass",
    )


@pytest.fixture
def db():
    database = Database(":memory:")
    yield database
    database.close()


@pytest.fixture
def harness(db):
    bus = EventBus()
    queue = TaskQueue(db, bus)
    dfs = FakeDFS()
    llm = FakeLLM()
    pipeline = Pipeline(db, queue, dfs, llm, poll_delay=0.01, poll_max_attempts=5)
    return {
        "db": db,
        "bus": bus,
        "queue": queue,
        "dfs": dfs,
        "llm": llm,
        "pipeline": pipeline,
    }
