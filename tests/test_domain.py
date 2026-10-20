from concurrent.futures import ThreadPoolExecutor

import pytest

from app.domainutil import normalize_domain


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Example.COM", "example.com"),
        ("https://www.example.com/path?q=1", "example.com"),
        ("http://example.com:8080", "example.com"),
        ("WWW.ACME.IN", "acme.in"),
    ],
)
def test_normalize_domain(raw, expected):
    assert normalize_domain(raw) == expected


@pytest.mark.parametrize(
    "raw",
    ["", "not-a-domain", "http://", "localhost", "127.0.0.1", "user@acme.in", "example.c"],
)
def test_normalize_rejects_invalid(raw):
    with pytest.raises(ValueError):
        normalize_domain(raw)


def test_submit_keeps_only_valid_domains(harness):
    items = harness["pipeline"].submit(
        ["acme.in, notes", "https://www.Example.com/about", "hello", "127.0.0.1"]
    )
    by_domain = {item["domain"]: item["action"] for item in items}
    assert by_domain["acme.in"] == "queued"
    assert by_domain["example.com"] == "queued"
    assert by_domain["notes"] == "skipped"
    assert by_domain["hello"] == "skipped"
    assert by_domain["127.0.0.1"] == "skipped"
    stored = {
        row["domain"]
        for row in harness["db"].fetch_dicts("SELECT domain FROM domains")
    }
    assert stored == {"acme.in", "example.com"}
    with pytest.raises(ValueError, match="valid websites"):
        harness["pipeline"].submit(["not a domain", "localhost"])


def test_submit_deduplicates_and_blocks_concurrent(harness):
    pipeline = harness["pipeline"]
    first = pipeline.submit(["https://www.Acme.in/about"])[0]
    second = pipeline.submit(["acme.in"])[0]
    assert first["action"] == "queued"
    assert second["action"] == "in_progress"
    assert first["domain"] == second["domain"] == "acme.in"

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: pipeline.submit(["acme.in"])[0], range(8)))
    assert all(r["action"] in {"in_progress", "duplicate_in_batch"} for r in results)


def test_cached_unless_force(harness):
    pipeline = harness["pipeline"]
    domain = "cached.test"
    pipeline.submit([domain])
    harness["db"].execute(
        "UPDATE domains SET pipeline_status = 'completed' WHERE domain = ?",
        [domain],
    )
    cached = pipeline.submit([domain])[0]
    assert cached["action"] == "cached"
    forced = pipeline.submit([domain], force=True)[0]
    assert forced["action"] == "queued"
