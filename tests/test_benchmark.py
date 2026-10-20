import time

from app.domainutil import normalize_domain


def test_dedup_insert_benchmark(harness):
    pipeline = harness["pipeline"]
    domains = [f"site{i}.example" for i in range(200)]
    t0 = time.perf_counter()
    pipeline.submit(domains)
    first_ms = (time.perf_counter() - t0) * 1000
    t1 = time.perf_counter()
    again = pipeline.submit(domains)
    second_ms = (time.perf_counter() - t1) * 1000
    assert all(item["action"] == "in_progress" for item in again)
    assert second_ms <= first_ms * 4 or second_ms < 250
    print(f"\nbenchmark submit-200 first={first_ms:.1f}ms cached={second_ms:.1f}ms")


def test_normalize_benchmark():
    values = [f"https://www.Company-{i}.in/path" for i in range(5000)]
    t0 = time.perf_counter()
    out = [normalize_domain(v) for v in values]
    elapsed_ms = (time.perf_counter() - t0) * 1000
    assert len(set(out)) == 5000
    print(f"\nbenchmark normalize-5000 {elapsed_ms:.1f}ms")
    assert elapsed_ms < 1000
