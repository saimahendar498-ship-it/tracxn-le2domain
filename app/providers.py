from __future__ import annotations

import hashlib
import logging
import random
import threading
import time
from datetime import timedelta
from typing import Any, Protocol

import httpx
from openai import OpenAI

from app.config import Settings
from app.db import Database, dumps, loads, utcnow
from app.jsonutil import parse_extract_result, parse_validate_result

log = logging.getLogger("anid2le.providers")

DFS_READY = {20000}
DFS_PENDING = {20100, 40601, 40602, 40603, 50000, 40102}


class DataForSEOClient:
    def __init__(self, settings: Settings, db: Database, http: httpx.Client | None = None):
        self.settings = settings
        self.db = db
        self.http = http or httpx.Client(timeout=60.0)

    def _auth(self) -> tuple[str, str]:
        if not self.settings.dfs_login or not self.settings.dfs_password:
            raise RuntimeError("DFS_LOGIN and DFS_PASSWORD are required")
        return (self.settings.dfs_login, self.settings.dfs_password)

    def _cache_get(self, key: str) -> dict[str, Any] | None:
        row = self.db.fetch_dict(
            "SELECT response_json, expires_at FROM api_cache WHERE cache_key = ?",
            [key],
        )
        if not row:
            return None
        if row["expires_at"] and row["expires_at"] < utcnow():
            self.db.execute("DELETE FROM api_cache WHERE cache_key = ?", [key])
            return None
        return loads(row["response_json"])

    def _cache_put(self, key: str, provider: str, payload: dict[str, Any]) -> None:
        now = utcnow()
        expires = now + timedelta(seconds=self.settings.cache_ttl_seconds)
        self.db.execute(
            """
            INSERT OR REPLACE INTO api_cache (cache_key, provider, response_json, created_at, expires_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            [key, provider, dumps(payload), now, expires],
        )

    def post_ai_mode(self, keyword: str, *, use_cache: bool = True) -> dict[str, Any]:
        keyword = (keyword or "")[: self.settings.dfs_keyword_max_chars]
        cache_key = "dfs:post:" + hashlib.sha256(keyword.encode("utf-8")).hexdigest()
        if use_cache:
            cached = self._cache_get(cache_key)
            if cached:
                return cached
        payload = [
            {
                "keyword": keyword,
                "location_code": self.settings.dfs_location_code,
                "language_code": self.settings.dfs_language_code,
                "priority": self.settings.dfs_priority,
            }
        ]
        response = self.http.post(
            "https://api.dataforseo.com/v3/serp/google/ai_mode/task_post",
            auth=self._auth(),
            json=payload,
        )
        response.raise_for_status()
        data = response.json()
        task = (data.get("tasks") or [None])[0]
        if not task:
            raise RuntimeError(f"DataForSEO post returned no tasks: {data}")
        code = task.get("status_code")
        if code not in DFS_READY | {20100}:
            raise RuntimeError(f"DataForSEO post failed: {code} {task.get('status_message')}")
        result = {
            "task_id": task.get("id"),
            "cost": data.get("cost"),
            "status_code": code,
            "raw": data,
        }
        if not result["task_id"]:
            raise RuntimeError("DataForSEO post succeeded without a task id")
        if use_cache:
            self._cache_put(cache_key, "dataforseo_post", result)
        return result

    def get_ai_mode(self, task_id: str, *, use_cache: bool = True) -> dict[str, Any]:
        cache_key = "dfs:get:" + task_id
        if use_cache:
            cached = self._cache_get(cache_key)
            if cached:
                return cached
        response = self.http.get(
            f"https://api.dataforseo.com/v3/serp/google/ai_mode/task_get/advanced/{task_id}",
            auth=self._auth(),
        )
        response.raise_for_status()
        data = response.json()
        task = (data.get("tasks") or [None])[0]
        if not task:
            raise RuntimeError(f"DataForSEO get returned no tasks: {data}")
        code = int(task.get("status_code") or 0)
        result = {"status_code": code, "task": task, "raw": data}
        if code in DFS_READY:
            if use_cache:
                self._cache_put(cache_key, "dataforseo_get", result)
            return result
        if code in DFS_PENDING:
            result["pending"] = True
            return result
        raise RuntimeError(f"DataForSEO get failed: {code} {task.get('status_message')}")


class LLMClient:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._local = threading.local()
        self._print_lock = threading.Lock()

    def _clients(self) -> dict[str, OpenAI]:
        clients = getattr(self._local, "clients", None)
        if clients is None:
            clients = {}
            self._local.clients = clients
        return clients

    def _client(self, api_key: str) -> OpenAI:
        clients = self._clients()
        if api_key not in clients:
            clients[api_key] = OpenAI(
                base_url=self.settings.nvidia_base_url,
                api_key=api_key,
                max_retries=0,
                timeout=60.0,
            )
        return clients[api_key]

    def complete_json(
        self,
        prompt: str,
        *,
        system: str | None,
        parser,
        seed: int = 0,
        max_key_attempts: int = 5,
        max_retries_per_key: int = 2,
    ) -> dict[str, Any]:
        keys = self.settings.nvidia_api_keys
        if not keys:
            raise RuntimeError("NVIDIA_API_KEYS is required")
        start_index = (seed + random.randrange(len(keys))) % len(keys)
        attempts = min(max_key_attempts, len(keys))
        last_error: Exception | None = None
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        for key_offset in range(attempts):
            api_key = keys[(start_index + key_offset) % len(keys)]
            for retry in range(max_retries_per_key):
                try:
                    response = self._client(api_key).chat.completions.create(
                        model=self.settings.nvidia_model,
                        messages=messages,
                        temperature=0,
                        max_tokens=1500,
                        stream=False,
                    )
                    content = response.choices[0].message.content
                    if not content:
                        raise ValueError("The API returned an empty response.")
                    return parser(content)
                except Exception as exc:
                    last_error = exc
                    log.warning(
                        "LLM retry key=%s attempt=%s error=%s",
                        (start_index + key_offset) % len(keys) + 1,
                        retry + 1,
                        exc,
                    )
                    status_code = getattr(exc, "status_code", None)
                    if status_code in (400, 401, 403, 404, 422):
                        break
                    if retry < max_retries_per_key - 1:
                        time.sleep(min(2 ** retry, 8))
        raise RuntimeError(f"LLM extraction failed: {last_error}")

    def extract_entity(self, serp_text: str, seed: int = 0) -> dict[str, Any]:
        from app.prompts import EXTRACT_PROMPT, EXTRACT_SYSTEM

        return self.complete_json(
            EXTRACT_PROMPT + serp_text,
            system=EXTRACT_SYSTEM,
            parser=parse_extract_result,
            seed=seed,
            max_key_attempts=5,
            max_retries_per_key=2,
        )

    def extract_validation(self, serp_text: str, seed: int = 0) -> dict[str, Any]:
        from app.prompts import VALIDATE_EXTRACT_PROMPT, VALIDATE_SYSTEM

        keys = self.settings.nvidia_api_keys
        return self.complete_json(
            VALIDATE_EXTRACT_PROMPT + serp_text,
            system=VALIDATE_SYSTEM,
            parser=parse_validate_result,
            seed=seed,
            max_key_attempts=max(1, len(keys)),
            max_retries_per_key=5,
        )


class Clock(Protocol):
    def sleep(self, seconds: float) -> None: ...
