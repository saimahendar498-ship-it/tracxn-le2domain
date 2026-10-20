from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

from app.db import Database, dumps, utcnow
from app.domainutil import domain_tokens, normalize_domain
from app.prompts import IDENTIFY_PROMPT, RETRY_PROMPT, VALIDATE_PROMPT
from app.providers import DataForSEOClient, LLMClient
from app.queue import ACTIVE_DOMAIN_STATUSES, TaskQueue
from app.serp import extract_answer, serp_for_llm

log = logging.getLogger("anid2le.pipeline")

STAGE_IDENTIFY_PROMPT = "identify_prompt"
STAGE_SERP_POST_1 = "serp_post_1"
STAGE_SERP_POLL_1 = "serp_poll_1"
STAGE_EXTRACT_1 = "extract_1"
STAGE_RETRY_PROMPT = "retry_prompt"
STAGE_SERP_POST_2 = "serp_post_2"
STAGE_SERP_POLL_2 = "serp_poll_2"
STAGE_EXTRACT_2 = "extract_2"
STAGE_VALIDATE_PROMPT = "validate_prompt"
STAGE_SERP_POST_3 = "serp_post_3"
STAGE_SERP_POLL_3 = "serp_poll_3"
STAGE_EXTRACT_VALIDATE = "extract_validate"

PIPELINE_STAGES = [
    STAGE_IDENTIFY_PROMPT,
    STAGE_SERP_POST_1,
    STAGE_SERP_POLL_1,
    STAGE_EXTRACT_1,
    STAGE_RETRY_PROMPT,
    STAGE_SERP_POST_2,
    STAGE_SERP_POLL_2,
    STAGE_EXTRACT_2,
    STAGE_VALIDATE_PROMPT,
    STAGE_SERP_POST_3,
    STAGE_SERP_POLL_3,
    STAGE_EXTRACT_VALIDATE,
]


class Waiting(Exception):
    def __init__(self, seconds: float, note: str = "serp pending"):
        super().__init__(note)
        self.seconds = seconds
        self.note = note


class Pipeline:
    def __init__(
        self,
        db: Database,
        queue: TaskQueue,
        dfs: DataForSEOClient,
        llm: LLMClient,
        poll_delay: float = 8.0,
        poll_max_attempts: int = 60,
    ):
        self.db = db
        self.queue = queue
        self.dfs = dfs
        self.llm = llm
        self.poll_delay = poll_delay
        self.poll_max_attempts = poll_max_attempts

    def submit(
        self,
        raw_domains: list[str],
        *,
        force: bool = False,
        priority: int = 100,
        use_cache: bool = True,
    ) -> list[dict[str, Any]]:
        now = utcnow()
        items: list[dict[str, Any] | None] = []
        unique: list[tuple[int, str, str]] = []
        seen: set[str] = set()
        for raw in domain_tokens(raw_domains):
            try:
                domain = normalize_domain(raw)
            except ValueError:
                shown = raw.strip()
                if len(shown) > 80:
                    shown = shown[:77] + "..."
                items.append(
                    {
                        "domain": shown,
                        "action": "skipped",
                        "message": "Not a valid website.",
                        "pipeline_status": None,
                        "current_stage": None,
                        "active_task_id": None,
                        "result": None,
                    }
                )
                continue
            if domain in seen:
                items.append(
                    {
                        "domain": domain,
                        "action": "duplicate_in_batch",
                        "message": "This website is already in the list you just sent.",
                        "pipeline_status": None,
                        "current_stage": None,
                        "active_task_id": None,
                        "result": None,
                    }
                )
                continue
            seen.add(domain)
            unique.append((len(items), domain, raw))
            items.append(None)

        if not unique:
            raise ValueError("None of those are valid websites.")

        domains = [domain for _, domain, _ in unique]
        placeholders = ",".join("?" * len(domains))
        with self.db.cursor():
            return self._submit_locked(
                items, unique, domains, placeholders, now, force, priority, use_cache
            )

    def _submit_locked(
        self,
        items,
        unique,
        domains,
        placeholders,
        now,
        force,
        priority,
        use_cache,
    ):
        existing = {
            row["domain"]: row
            for row in self.db.fetch_dicts(
                f"""
                SELECT domain, pipeline_status, current_stage, active_task_id
                FROM domains WHERE domain IN ({placeholders})
                """,
                domains,
            )
        }

        inserts: list[tuple] = []
        result_inserts: list[tuple] = []
        updates: list[tuple] = []
        reset_domains: list[str] = []
        queue_domains: list[str] = []
        use_cache_flag = use_cache and not force

        for idx, domain, raw in unique:
            row = existing.get(domain)
            if row and not force:
                status = row["pipeline_status"]
                if status in ACTIVE_DOMAIN_STATUSES:
                    items[idx] = self._light(
                        domain, "in_progress", row, "Already working on this website."
                    )
                    continue
                if status == "completed":
                    items[idx] = self._light(
                        domain,
                        "cached",
                        row,
                        "Already finished. Tick “Look up again” if you want a fresh search.",
                    )
                    continue
                if status != "failed":
                    items[idx] = self._light(
                        domain, "exists", row, "This website is already saved."
                    )
                    continue
            if row is None:
                inserts.append((domain, raw, STAGE_IDENTIFY_PROMPT, now, now))
                result_inserts.append((domain, now))
            else:
                updates.append((raw, STAGE_IDENTIFY_PROMPT, now, domain))
                if force:
                    reset_domains.append(domain)
                    result_inserts.append((domain, now))
            queue_domains.append(domain)

        self.db.executemany(
            """
            INSERT INTO domains (
                domain, input_raw, pipeline_status, current_stage, error, created_at, updated_at
            ) VALUES (?, ?, 'queued', ?, NULL, ?, ?)
            """,
            inserts,
        )
        if updates:
            self.db.executemany(
                """
                UPDATE domains
                SET input_raw = ?, pipeline_status = 'queued', current_stage = ?,
                    error = NULL, active_task_id = NULL, updated_at = ?
                WHERE domain = ?
                """,
                updates,
            )
        if reset_domains:
            reset_sql = ",".join("?" * len(reset_domains))
            self.db.execute(
                f"DELETE FROM domain_results WHERE domain IN ({reset_sql})",
                reset_domains,
            )
        self.db.executemany(
            "INSERT INTO domain_results (domain, updated_at) VALUES (?, ?)",
            result_inserts,
        )

        if queue_domains:
            tasks = self.queue.enqueue_many(
                [
                    {
                        "job_type": "pipeline_stage",
                        "domain": domain,
                        "stage": STAGE_IDENTIFY_PROMPT,
                        "priority": priority,
                        "payload": {"use_cache": use_cache_flag, "poll_attempts": 0},
                    }
                    for domain in queue_domains
                ]
            )
            self.db.executemany(
                "UPDATE domains SET active_task_id = ?, updated_at = ? WHERE domain = ?",
                [(task["id"], now, task["domain"]) for task in tasks],
            )
            by_domain = {task["domain"]: task for task in tasks}
            for idx, domain, _raw in unique:
                task = by_domain.get(domain)
                if not task:
                    continue
                items[idx] = {
                    "domain": domain,
                    "action": "queued",
                    "message": "Started. This website is in line.",
                    "pipeline_status": "queued",
                    "current_stage": STAGE_IDENTIFY_PROMPT,
                    "active_task_id": task["id"],
                    "result": None,
                }
            self.queue.bus.set_snapshot(self.queue.snapshot())

        return [item for item in items if item is not None]

    def _light(
        self, domain: str, action: str, row: dict[str, Any], message: str
    ) -> dict[str, Any]:
        return {
            "domain": domain,
            "action": action,
            "message": message,
            "pipeline_status": row.get("pipeline_status"),
            "current_stage": row.get("current_stage"),
            "active_task_id": row.get("active_task_id"),
            "result": None,
        }

    def _domain_payload(
        self,
        domain: str,
        *,
        action: str,
        existing: dict[str, Any] | None = None,
        task: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        row = existing or self.db.fetch_dict(
            "SELECT * FROM domains WHERE domain = ?", [domain]
        )
        result = self.db.fetch_dict(
            "SELECT * FROM domain_results WHERE domain = ?", [domain]
        )
        return {
            "domain": domain,
            "action": action,
            "pipeline_status": (row or {}).get("pipeline_status"),
            "current_stage": (row or {}).get("current_stage"),
            "active_task_id": (row or {}).get("active_task_id") or (task or {}).get("id"),
            "result": self._public_result(result),
        }

    def get_domain(self, raw: str) -> dict[str, Any]:
        domain = normalize_domain(raw)
        payload = self._domain_payload(domain, action="get")
        if payload["pipeline_status"] is None:
            raise KeyError(domain)
        return payload

    def get_many(
        self,
        domains: list[str] | None = None,
        ids: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        names = [str(item) for item in (domains or [])]
        task_ids = [str(item).strip() for item in (ids or []) if str(item).strip()]
        if not names and not task_ids:
            raise ValueError("Pass at least one domain name or id.")

        id_to_domain: dict[str, str | None] = {}
        if task_ids:
            placeholders = ",".join("?" * len(task_ids))
            for row in self.db.fetch_dicts(
                f"SELECT id, domain FROM tasks WHERE id IN ({placeholders})",
                task_ids,
            ):
                id_to_domain[row["id"]] = row.get("domain")

        wanted: list[tuple[str, str, str | None]] = []
        keys: list[str] = []
        for raw in names:
            try:
                domain = normalize_domain(raw)
            except ValueError:
                wanted.append(("domain", raw.strip(), None))
                continue
            wanted.append(("domain", domain, domain))
            keys.append(domain)
        for task_id in task_ids:
            domain = id_to_domain.get(task_id)
            if domain:
                keys.append(domain)
            wanted.append(("id", task_id, domain))

        existing: dict[str, dict[str, Any]] = {}
        results: dict[str, dict[str, Any]] = {}
        unique = list(dict.fromkeys(keys))
        if unique:
            placeholders = ",".join("?" * len(unique))
            for row in self.db.fetch_dicts(
                f"SELECT * FROM domains WHERE domain IN ({placeholders})",
                unique,
            ):
                existing[row["domain"]] = row
            for row in self.db.fetch_dicts(
                f"SELECT * FROM domain_results WHERE domain IN ({placeholders})",
                unique,
            ):
                results[row["domain"]] = row

        items: list[dict[str, Any]] = []
        for kind, raw, domain in wanted:
            row = existing.get(domain) if domain else None
            if not row:
                item = {
                    "found": False,
                    "domain": domain or raw,
                    "pipeline_status": None,
                    "current_stage": None,
                    "active_task_id": None,
                    "error": None,
                    "result": None,
                }
                if kind == "id":
                    item["id"] = raw
                elif domain is None:
                    item["message"] = "Not a valid website."
                items.append(item)
                continue
            item = {
                "found": True,
                "domain": domain,
                "pipeline_status": row.get("pipeline_status"),
                "current_stage": row.get("current_stage"),
                "active_task_id": row.get("active_task_id"),
                "error": row.get("error"),
                "result": self._public_result(results.get(domain)),
            }
            if kind == "id":
                item["id"] = raw
            items.append(item)
        return items

    def list_domains(self, limit: int = 200) -> list[dict[str, Any]]:
        rows = self.db.fetch_dicts(
            """
            SELECT d.*, r.le_name_final, r.entity_type_final, r.status_final, r.outcome, r.status_1
            FROM domains d
            LEFT JOIN domain_results r ON r.domain = d.domain
            ORDER BY d.updated_at DESC
            LIMIT ?
            """,
            [limit],
        )
        return rows

    def run_task(self, task: dict[str, Any]) -> dict[str, Any] | None:
        job_type = task["job_type"]
        if job_type == "pipeline_stage":
            return self._run_stage(task)
        if job_type == "reprocess_pending":
            pending = self.db.fetch_dicts(
                "SELECT domain, input_raw FROM domains WHERE pipeline_status IN ('pending', 'failed')"
            )
            submitted = self.submit(
                [r["input_raw"] or r["domain"] for r in pending], force=False
            )
            return {"submitted": len(submitted)}
        raise ValueError(f"unknown job_type {job_type}")

    def retry_failed(self, *, min_age_seconds: float = 0) -> int:
        cutoff = utcnow() - timedelta(seconds=min_age_seconds)
        rows = self.db.fetch_dicts(
            """
            SELECT domain, input_raw FROM domains
            WHERE pipeline_status = 'failed' AND updated_at <= ?
            """,
            [cutoff],
        )
        if not rows:
            return 0
        self.submit([row["input_raw"] or row["domain"] for row in rows], force=False)
        return len(rows)

    def delete_domain(self, raw: str) -> dict[str, Any]:
        domain = normalize_domain(raw)
        row = self.db.fetchone("SELECT 1 FROM domains WHERE domain = ?", [domain])
        if not row:
            raise KeyError(domain)
        now = utcnow()
        self.db.execute(
            """
            UPDATE tasks
            SET status = 'cancelled', updated_at = ?, finished_at = ?,
                locked_at = NULL, locked_by = NULL
            WHERE domain = ? AND status IN ('queued', 'running', 'waiting')
            """,
            [now, now, domain],
        )
        self.db.execute("DELETE FROM tasks WHERE domain = ?", [domain])
        self.db.execute("DELETE FROM domain_results WHERE domain = ?", [domain])
        self.db.execute("DELETE FROM domains WHERE domain = ?", [domain])
        self.queue.bus.set_snapshot(self.queue.snapshot())
        return {"domain": domain, "deleted": True}

    def _domain_exists(self, domain: str) -> bool:
        return self.db.fetchone("SELECT 1 FROM domains WHERE domain = ?", [domain]) is not None

    def _run_stage(self, task: dict[str, Any]) -> dict[str, Any] | None:
        domain = task["domain"]
        if domain and not self._domain_exists(domain):
            return {"domain": domain, "skipped": "deleted"}
        stage = task["stage"]
        payload = task.get("payload") or {}
        use_cache = bool(payload.get("use_cache", True))
        now = utcnow()
        self.db.execute(
            """
            UPDATE domains
            SET pipeline_status = 'running', current_stage = ?, active_task_id = ?, updated_at = ?
            WHERE domain = ?
            """,
            [stage, task["id"], now, domain],
        )
        row = self._result_row(domain)
        handlers = {
            STAGE_IDENTIFY_PROMPT: self._identify_prompt,
            STAGE_SERP_POST_1: lambda d, r, c: self._serp_post(d, r, c, 1),
            STAGE_SERP_POLL_1: lambda d, r, c: self._serp_poll(d, r, c, 1, payload),
            STAGE_EXTRACT_1: lambda d, r, c: self._extract(d, r, c, 1, task),
            STAGE_RETRY_PROMPT: self._retry_prompt,
            STAGE_SERP_POST_2: lambda d, r, c: self._serp_post(d, r, c, 2),
            STAGE_SERP_POLL_2: lambda d, r, c: self._serp_poll(d, r, c, 2, payload),
            STAGE_EXTRACT_2: lambda d, r, c: self._extract(d, r, c, 2, task),
            STAGE_VALIDATE_PROMPT: self._validate_prompt,
            STAGE_SERP_POST_3: lambda d, r, c: self._serp_post(d, r, c, 3),
            STAGE_SERP_POLL_3: lambda d, r, c: self._serp_poll(d, r, c, 3, payload),
            STAGE_EXTRACT_VALIDATE: self._extract_validate,
        }
        handler = handlers.get(stage)
        if not handler:
            raise ValueError(f"unknown stage {stage}")
        handler(domain, row, use_cache)
        if domain and not self._domain_exists(domain):
            return {"domain": domain, "skipped": "deleted"}
        next_stage = self._next_stage(domain, stage)
        if next_stage is None:
            self.db.execute(
                """
                UPDATE domains
                SET pipeline_status = 'completed', current_stage = 'completed',
                    error = NULL, updated_at = ?
                WHERE domain = ?
                """,
                [utcnow(), domain],
            )
            return {"domain": domain, "stage": stage, "done": True}
        nxt = self.queue.enqueue(
            "pipeline_stage",
            domain=domain,
            stage=next_stage,
            payload={"use_cache": use_cache, "poll_attempts": 0},
            priority=task.get("priority") or 100,
        )
        self.db.execute(
            "UPDATE domains SET current_stage = ?, active_task_id = ?, updated_at = ? WHERE domain = ?",
            [next_stage, nxt["id"], utcnow(), domain],
        )
        return {"domain": domain, "stage": stage, "next": next_stage}

    def fail_domain(self, domain: str, error: str) -> None:
        self.db.execute(
            """
            UPDATE domains
            SET pipeline_status = 'failed', error = ?, updated_at = ?
            WHERE domain = ?
            """,
            [error, utcnow(), domain],
        )

    def _result_row(self, domain: str) -> dict[str, Any]:
        row = self.db.fetch_dict(
            "SELECT * FROM domain_results WHERE domain = ?", [domain]
        )
        if not row:
            self.db.execute(
                "INSERT INTO domain_results (domain, updated_at) VALUES (?, ?)",
                [domain, utcnow()],
            )
            row = self.db.fetch_dict(
                "SELECT * FROM domain_results WHERE domain = ?", [domain]
            )
        return row or {"domain": domain}

    def _update_result(self, domain: str, **fields: Any) -> None:
        if not fields:
            return
        fields["updated_at"] = utcnow()
        assignments = ", ".join(f"{k} = ?" for k in fields)
        values = list(fields.values()) + [domain]
        self.db.execute(
            f"UPDATE domain_results SET {assignments} WHERE domain = ?",
            values,
        )

    def _identify_prompt(self, domain: str, row: dict[str, Any], use_cache: bool) -> None:
        self._update_result(domain, prompt_identify=IDENTIFY_PROMPT.format(domain=domain))

    def _retry_prompt(self, domain: str, row: dict[str, Any], use_cache: bool) -> None:
        if not self._needs_retry(row):
            return
        self._update_result(domain, prompt_retry=RETRY_PROMPT.format(domain=domain))

    def _validate_prompt(self, domain: str, row: dict[str, Any], use_cache: bool) -> None:
        if not self._needs_validate(row):
            return
        le_name = row.get("le_name_1") or ""
        entity_type = row.get("entity_type_1") or ""
        if not le_name:
            le_name = row.get("le_name_2") or ""
            entity_type = row.get("entity_type_2") or ""
        if not le_name:
            le_name = "Null"
            entity_type = "Null"
        self._update_result(
            domain,
            prompt_validate=VALIDATE_PROMPT.format(
                domain=domain, le_name=le_name, entity_type=entity_type
            ),
        )

    def _serp_post(self, domain: str, row: dict[str, Any], use_cache: bool, pass_no: int) -> None:
        if pass_no == 2 and not self._needs_retry(row):
            return
        if pass_no == 3 and not self._needs_validate(row):
            return
        prompt_field = {1: "prompt_identify", 2: "prompt_retry", 3: "prompt_validate"}[pass_no]
        keyword = row.get(prompt_field) or ""
        posted = self.dfs.post_ai_mode(keyword, use_cache=use_cache)
        self._update_result(
            domain,
            **{
                f"dfs_task_id_{pass_no}": posted["task_id"],
                f"dfs_cost_{pass_no}": posted.get("cost"),
            },
        )

    def _serp_poll(
        self,
        domain: str,
        row: dict[str, Any],
        use_cache: bool,
        pass_no: int,
        payload: dict[str, Any],
    ) -> None:
        if pass_no == 2 and not self._needs_retry(row):
            return
        if pass_no == 3 and not self._needs_validate(row):
            return
        task_id = row.get(f"dfs_task_id_{pass_no}")
        if not task_id:
            raise RuntimeError(f"missing DataForSEO task id for pass {pass_no}")
        got = self.dfs.get_ai_mode(task_id, use_cache=use_cache)
        if got.get("pending"):
            attempts = int(payload.get("poll_attempts") or 0) + 1
            if attempts > self.poll_max_attempts:
                raise RuntimeError(f"DataForSEO task {task_id} timed out")
            payload["poll_attempts"] = attempts
            raise Waiting(self.poll_delay, f"serp pass {pass_no} pending ({attempts})")
        full, markdown = extract_answer(got["task"])
        self._update_result(
            domain,
            **{
                f"serp_text_{pass_no}": full,
                f"serp_markdown_{pass_no}": markdown,
            },
        )

    def _extract(
        self,
        domain: str,
        row: dict[str, Any],
        use_cache: bool,
        pass_no: int,
        task: dict[str, Any],
    ) -> None:
        if pass_no == 2 and not self._needs_retry(row):
            return
        text = serp_for_llm(
            row.get(f"serp_text_{pass_no}") or "",
            row.get(f"serp_markdown_{pass_no}") or "",
        )
        parsed = self.llm.extract_entity(text, seed=hash(domain) % 10_000)
        self._update_result(
            domain,
            **{
                f"extract_json_{pass_no}": dumps(parsed),
                f"le_name_{pass_no}": parsed["LE_NAME"],
                f"entity_type_{pass_no}": parsed["ENTITY_TYPE"],
                f"source_urls_{pass_no}": ",".join(parsed["SOURCE_URLS"]),
                f"evidence_{pass_no}": parsed["EVIDENCE"],
                f"status_{pass_no}": parsed["STATUS"],
            },
        )

    def _extract_validate(
        self, domain: str, row: dict[str, Any], use_cache: bool
    ) -> None:
        if not self._needs_validate(row):
            return
        text = serp_for_llm(row.get("serp_text_3") or "", row.get("serp_markdown_3") or "")
        parsed = self.llm.extract_validation(text, seed=hash(domain) % 10_000)
        self._update_result(
            domain,
            extract_json_3=dumps(parsed),
            le_name_final=parsed["LE_NAME"],
            entity_type_final=parsed["ENTITY_TYPE"],
            status_final=parsed["STATUS"],
            source_urls_final=",".join(parsed["SOURCE_URLS"]),
            evidence_final=parsed["EVIDENCE"],
            outcome=parsed["OUTCOME"],
        )

    def _needs_retry(self, row: dict[str, Any]) -> bool:
        entity_type = str(row.get("entity_type_1") or "").lower().strip()
        status = row.get("status_1") or ""
        return entity_type == "other" or status == "NOT_FOUND"

    def _needs_validate(self, row: dict[str, Any]) -> bool:
        return bool(row.get("status_1"))

    def _next_stage(self, domain: str, current: str) -> str | None:
        row = self._result_row(domain)
        idx = PIPELINE_STAGES.index(current)
        for stage in PIPELINE_STAGES[idx + 1 :]:
            if stage in (STAGE_RETRY_PROMPT, STAGE_SERP_POST_2, STAGE_SERP_POLL_2, STAGE_EXTRACT_2):
                if not self._needs_retry(row):
                    continue
            if stage in (
                STAGE_VALIDATE_PROMPT,
                STAGE_SERP_POST_3,
                STAGE_SERP_POLL_3,
                STAGE_EXTRACT_VALIDATE,
            ):
                if not self._needs_validate(row):
                    continue
            return stage
        return None

    def _public_result(self, result: dict[str, Any] | None) -> dict[str, Any] | None:
        if not result:
            return None
        keys = [
            "le_name_1",
            "entity_type_1",
            "status_1",
            "source_urls_1",
            "evidence_1",
            "le_name_2",
            "entity_type_2",
            "status_2",
            "le_name_final",
            "entity_type_final",
            "status_final",
            "source_urls_final",
            "evidence_final",
            "outcome",
            "dfs_task_id_1",
            "dfs_task_id_2",
            "dfs_task_id_3",
        ]
        return {k: result.get(k) for k in keys}
