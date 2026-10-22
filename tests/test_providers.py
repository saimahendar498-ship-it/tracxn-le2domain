import base64

import httpx
import pytest

from app.config import Settings
from app.db import Database
from app.providers import DataForSEOClient


def _login(request: httpx.Request) -> str:
    token = request.headers["authorization"].split(" ", 1)[1]
    return base64.b64decode(token).decode().split(":", 1)[0]


def _client(handler, login="one,two", password="a,b"):
    settings = Settings(
        dfs_login=login,
        dfs_password=password,
        dfs_api_keys="",
        duckdb_path=":memory:",
        nvidia_api_keys=["k"],
    )
    db = Database(":memory:")
    http = httpx.Client(transport=httpx.MockTransport(handler))
    return DataForSEOClient(settings, db, http=http), db


def test_post_uses_the_next_account_when_the_first_is_rejected():
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        user = _login(request)
        seen.append(user)
        if user == "one":
            return httpx.Response(401, json={"tasks": []})
        return httpx.Response(
            200,
            json={"cost": 0.01, "tasks": [{"id": "task-2", "status_code": 20100}]},
        )

    client, db = _client(handler)
    posted = client.post_ai_mode("find the company", use_cache=False)
    assert seen == ["one", "two"]
    assert posted["login"] == "two"
    assert posted["task_id"] == "task-2"
    db.close()


def test_post_skips_an_account_that_is_out_of_credit():
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        user = _login(request)
        seen.append(user)
        if user == "one":
            return httpx.Response(
                200,
                json={"tasks": [{"status_code": 40202, "status_message": "rate limit"}]},
            )
        return httpx.Response(
            200,
            json={"cost": 0.01, "tasks": [{"id": "task-9", "status_code": 20100}]},
        )

    client, db = _client(handler)
    first = client.post_ai_mode("one search", use_cache=False)
    second = client.post_ai_mode("another search", use_cache=False)
    assert first["login"] == "two"
    assert second["login"] == "two"
    assert seen == ["one", "two", "two"]
    db.close()


def test_get_checks_the_account_that_created_the_task():
    seen = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(_login(request))
        return httpx.Response(
            200,
            json={"tasks": [{"id": "task-2", "status_code": 20000, "result": []}]},
        )

    client, db = _client(handler)
    got = client.get_ai_mode("task-2", use_cache=False, login="two")
    assert seen == ["two"]
    assert got["status_code"] == 20000
    db.close()


def test_uneven_accounts_are_rejected():
    settings = Settings(dfs_login="one,two", dfs_password="only-one", dfs_api_keys="")
    db = Database(":memory:")
    client = DataForSEOClient(settings, db, http=httpx.Client())
    with pytest.raises(RuntimeError, match="same number"):
        client.post_ai_mode("keyword", use_cache=False)
    db.close()
