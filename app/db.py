from __future__ import annotations

import json
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import duckdb

SCHEMA_VERSION = 2

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS domains (
    domain VARCHAR PRIMARY KEY,
    input_raw VARCHAR NOT NULL,
    pipeline_status VARCHAR NOT NULL DEFAULT 'pending',
    current_stage VARCHAR,
    active_task_id VARCHAR,
    error VARCHAR,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS domain_results (
    domain VARCHAR PRIMARY KEY,
    prompt_identify VARCHAR,
    dfs_task_id_1 VARCHAR,
    dfs_cost_1 DOUBLE,
    serp_text_1 VARCHAR,
    serp_markdown_1 VARCHAR,
    extract_json_1 VARCHAR,
    le_name_1 VARCHAR,
    entity_type_1 VARCHAR,
    source_urls_1 VARCHAR,
    evidence_1 VARCHAR,
    status_1 VARCHAR,
    prompt_retry VARCHAR,
    dfs_task_id_2 VARCHAR,
    dfs_cost_2 DOUBLE,
    serp_text_2 VARCHAR,
    serp_markdown_2 VARCHAR,
    extract_json_2 VARCHAR,
    le_name_2 VARCHAR,
    entity_type_2 VARCHAR,
    source_urls_2 VARCHAR,
    evidence_2 VARCHAR,
    status_2 VARCHAR,
    prompt_validate VARCHAR,
    dfs_task_id_3 VARCHAR,
    dfs_cost_3 DOUBLE,
    serp_text_3 VARCHAR,
    serp_markdown_3 VARCHAR,
    extract_json_3 VARCHAR,
    le_name_final VARCHAR,
    entity_type_final VARCHAR,
    status_final VARCHAR,
    source_urls_final VARCHAR,
    evidence_final VARCHAR,
    outcome VARCHAR,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
    id VARCHAR PRIMARY KEY,
    job_type VARCHAR NOT NULL,
    domain VARCHAR,
    stage VARCHAR,
    status VARCHAR NOT NULL,
    priority INTEGER NOT NULL DEFAULT 100,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 5,
    run_after TIMESTAMP NOT NULL,
    locked_at TIMESTAMP,
    locked_by VARCHAR,
    payload VARCHAR,
    result VARCHAR,
    error VARCHAR,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    started_at TIMESTAMP,
    finished_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS schedules (
    id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    cron_expr VARCHAR,
    interval_seconds INTEGER,
    job_type VARCHAR NOT NULL,
    payload VARCHAR,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    last_run_at TIMESTAMP,
    next_run_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS api_cache (
    cache_key VARCHAR PRIMARY KEY,
    provider VARCHAR NOT NULL,
    response_json VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL,
    expires_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id VARCHAR PRIMARY KEY,
    task_id VARCHAR,
    domain VARCHAR,
    event_type VARCHAR NOT NULL,
    payload VARCHAR,
    created_at TIMESTAMP NOT NULL
);
"""

INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_tasks_claim ON tasks(status, run_after, priority);
CREATE INDEX IF NOT EXISTS idx_tasks_domain ON tasks(domain);
CREATE INDEX IF NOT EXISTS idx_events_created ON events(created_at);
CREATE INDEX IF NOT EXISTS idx_schedules_next ON schedules(enabled, next_run_at);
"""


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


class Database:
    def __init__(self, path: str):
        self.path = path
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._con = duckdb.connect(path)
        self.migrate()

    def migrate(self) -> None:
        with self._lock:
            self._con.execute(SCHEMA_SQL)
            self._rebuild_events_if_needed()
            self._con.execute(INDEX_SQL)
            self._con.execute("DROP SEQUENCE IF EXISTS events_id_seq")
            row = self._con.execute(
                "SELECT COALESCE(MAX(version), 0) FROM schema_migrations"
            ).fetchone()
            current = int(row[0] if row else 0)
            if current < SCHEMA_VERSION:
                self._con.execute(
                    "INSERT INTO schema_migrations VALUES (?, ?) ON CONFLICT DO NOTHING",
                    [SCHEMA_VERSION, utcnow()],
                )

    def _rebuild_events_if_needed(self) -> None:
        info = self._con.execute("PRAGMA table_info('events')").fetchall()
        id_type = ""
        for row in info:
            if row[1] == "id":
                id_type = str(row[2]).upper()
        if id_type == "VARCHAR":
            return
        self._con.execute("DROP TABLE IF EXISTS events")
        self._con.execute(
            """
            CREATE TABLE events (
                id VARCHAR PRIMARY KEY,
                task_id VARCHAR,
                domain VARCHAR,
                event_type VARCHAR NOT NULL,
                payload VARCHAR,
                created_at TIMESTAMP NOT NULL
            )
            """
        )

    def close(self) -> None:
        with self._lock:
            self._con.close()

    @contextmanager
    def cursor(self) -> Iterator[duckdb.DuckDBPyConnection]:
        with self._lock:
            yield self._con

    def execute(self, sql: str, params: list[Any] | None = None):
        with self._lock:
            return self._con.execute(sql, params or [])

    def executemany(self, sql: str, rows: list[tuple]) -> None:
        if not rows:
            return
        with self._lock:
            self._con.executemany(sql, rows)

    def fetchall(self, sql: str, params: list[Any] | None = None) -> list[tuple]:
        with self._lock:
            return self._con.execute(sql, params or []).fetchall()

    def fetchone(self, sql: str, params: list[Any] | None = None) -> tuple | None:
        with self._lock:
            return self._con.execute(sql, params or []).fetchone()

    def fetch_dicts(self, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
        with self._lock:
            rel = self._con.execute(sql, params or [])
            cols = [c[0] for c in rel.description]
            return [dict(zip(cols, row)) for row in rel.fetchall()]

    def fetch_dict(self, sql: str, params: list[Any] | None = None) -> dict[str, Any] | None:
        rows = self.fetch_dicts(sql, params)
        return rows[0] if rows else None


def dumps(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False)


def loads(value: Any, default: Any = None) -> Any:
    if value in (None, ""):
        return default
    if isinstance(value, (dict, list)):
        return value
    return json.loads(value)
