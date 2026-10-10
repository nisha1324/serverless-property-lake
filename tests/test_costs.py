"""Offline checks on the cost arithmetic in scripts/estimate_costs.py."""
import json

import pytest

import estimate_costs as ec

MB, TB = ec.MB, ec.TB


def test_athena_bills_at_least_10_mb_rounded_up_to_the_mb():
    assert ec.athena_cost(1, 1, 5.0) == pytest.approx(10 * MB / TB * 5)
    assert ec.athena_cost(10.2 * MB, 1, 5.0) == pytest.approx(11 * MB / TB * 5)
    assert ec.athena_cost(20 * MB, 500, 5.0) == pytest.approx(500 * 20 * MB / TB * 5)


def test_requests_split_into_put_list_and_get_tiers():
    calls = ["CreateMultipartUpload", "UploadPart", "UploadPart", "CompleteMultipartUpload",
             "GetObject", "GetObject", "PutObject", "HeadObject"]
    assert ec.requests(calls) == (5, 3)


def test_estimate_adds_up_from_committed_prices():
    prices = json.loads(ec.PRICES.read_text())
    manifest = {"source_bytes": 16_000_000, "rows_in": 100_000, "rows_valid": 100_000,
                "outputs": {"curated_bytes": 2_000_000}}
    runs = [{"import_s": 1.0, "handler_s": 1.0, "duplicate_s": 0.1, "peak_rss_mb": 300, "manifest": manifest,
             "calls_processed": ["GetObject"] * 2 + ["PutObject"] * 3,
             "calls_duplicate": ["GetObject"] * 2}]
    e = ec.estimate(prices, runs, ["PutObject"])
    p = {k: v["usd"] for k, v in prices["unit_prices_usd"].items()}
    assert e["gb_s"] == pytest.approx((2.0 + 0.1) * ec.DURATION_SAFETY * ec.MEMORY_MB / 1024)
    assert e["lines"]["Lambda compute"] == pytest.approx(e["gb_s"] * p["lambda_gb_second_arm"])
    assert (e["tier1"], e["tier2"]) == (4, 4)
    assert e["total"] == pytest.approx(sum(e["lines"].values()))
    assert e["athena_csv"] > e["athena_lake"]  # same queries, bigger scans


def test_memory_assumption_matches_the_template():
    import yaml
    from test_deploy import TEMPLATE, CfnLoader
    template = yaml.load(TEMPLATE.read_text(), Loader=CfnLoader)
    assert template["Parameters"]["MemorySize"]["Default"] == ec.MEMORY_MB
