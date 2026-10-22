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
# Auth, payment, and rate-limit failures belong to one account. Try the next key.
DFS_ACCOUNT_FAIL = {40100, 40200, 40201, 40202, 40203, 40204, 40205, 40206, 40207}
DFS_POST_OK = DFS_READY | {20100}


def dfs_accounts(settings: Settings) -> list[tuple[str, str]]:
    """Login/password pairs from DFS_LOGIN and DFS_PASSWORD, then DFS_API_KEYS."""
    logins = [part.strip() for part in settings.dfs_login.split(",") if part.strip()]
    passwords = [part.strip() for part in settings.dfs_password.split(",") if part.strip()]
    if len(logins) != len(passwords):
        raise RuntimeError(
            "DFS_LOGIN and DFS_PASSWORD must list the same number of comma-separated accounts"
        )
    accounts = list(zip(logins, passwords))
    for part in [item.strip() for item in settings.dfs_api_keys.split(",") if item.strip()]:
        if ":" not in part:
            raise RuntimeError("Each DFS_API_KEYS value must be login:password")
        login, password = part.split(":", 1)
        login, password = login.strip(), password.strip()
        if login and password:
            accounts.append((login, password))
    unique: list[tuple[str, str]] = []
    seen: set[str] = set()
    for login, password in accounts:
        if login in seen:
            continue
        seen.add(login)
        unique.append((login, password))
    return unique


class DataForSEOClient:
    def __init__(self, settings: Settings, db: Database, http: httpx.Client | None = None):
        self.settings = settings
        self.db = db
        self.http = http or httpx.Client(timeout=60.0)
        self._lock = threading.Lock()
        self._bad_logins: set[str] = set()

    def _accounts(self) -> list[tuple[str, str]]:
        accounts = dfs_accounts(self.settings)
        if not accounts:
            raise RuntimeError("DFS_LOGIN and DFS_PASSWORD are required")
        return accounts

    def _ordered(self, preferred_login: str | None = None) -> list[tuple[str, str]]:
        accounts = self._accounts()
        with self._lock:
            if self._bad_logins and self._bad_logins >= {login for login, _ in accounts}:
                self._bad_logins.clear()
            bad = set(self._bad_logins)
        healthy = [item for item in accounts if item[0] not in bad]
        failed = [item for item in accounts if item[0] in bad]
        ordered = healthy + failed
        if preferred_login:
            preferred = [item for item in accounts if item[0] == preferred_login]
            others = [item for item in ordered if item[0] != preferred_login]
            ordered = preferred + others
        return ordered

    def _mark_bad(self, login: str) -> None:
        with self._lock:
            self._bad_logins.add(login)

    def _call(
        self,
        method: str,
        url: str,
        *,
        body: list | None = None,
        preferred_login: str | None = None,
        ok_codes: set[int],
    ) -> tuple[dict[str, Any], dict[str, Any], str]:
        last_error: Exception | None = None
        accounts = self._ordered(preferred_login)
        for index, (login, password) in enumerate(accounts, start=1):
            try:
                response = self.http.request(method, url, auth=(login, password), json=body)
            except httpx.HTTPError as exc:
                last_error = exc
                log.warning("DataForSEO account %s failed: %s", index, exc)
                continue
            if response.status_code in (401, 402, 403, 429):
                last_error = RuntimeError(f"DataForSEO HTTP {response.status_code}")
                log.warning("DataForSEO account %s rejected (%s), trying the next", index, response.status_code)
                self._mark_bad(login)
                continue
            try:
                response.raise_for_status()
                data = response.json()
            except Exception as exc:
                last_error = exc
                log.warning("DataForSEO account %s failed: %s", index, exc)
                continue
            task = (data.get("tasks") or [None])[0] or {}
            code = int(task.get("status_code") or 0)
            if code in DFS_ACCOUNT_FAIL:
                last_error = RuntimeError(
                    f"DataForSEO account failed: {code} {task.get('status_message')}"
                )
                log.warning("DataForSEO account %s failed (%s), trying the next", index, code)
                self._mark_bad(login)
                continue
            if code not in ok_codes:
                raise RuntimeError(f"DataForSEO failed: {code} {task.get('status_message')}")
            return data, task, login
        raise RuntimeError(f"DataForSEO failed on every account: {last_error}")

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
        data, task, login = self._call(
            "POST",
            "https://api.dataforseo.com/v3/serp/google/ai_mode/task_post",
            body=payload,
            ok_codes=DFS_POST_OK,
        )
        code = int(task.get("status_code") or 0)
        result = {
            "task_id": task.get("id"),
            "login": login,
            "cost": data.get("cost"),
            "status_code": code,
            "raw": data,
        }
        if not result["task_id"]:
            raise RuntimeError("DataForSEO post succeeded without a task id")
        if use_cache:
            self._cache_put(cache_key, "dataforseo_post", result)
        return result

    def get_ai_mode(
        self,
        task_id: str,
        *,
        use_cache: bool = True,
        login: str | None = None,
    ) -> dict[str, Any]:
        cache_key = "dfs:get:" + task_id
        if use_cache:
            cached = self._cache_get(cache_key)
            if cached:
                return cached
        data, task, used_login = self._call(
            "GET",
            f"https://api.dataforseo.com/v3/serp/google/ai_mode/task_get/advanced/{task_id}",
            preferred_login=login,
            ok_codes=DFS_READY | DFS_PENDING,
        )
        code = int(task.get("status_code") or 0)
        result = {"status_code": code, "task": task, "raw": data, "login": used_login}
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
