"""Monthly AWS cost of running the lake, from measured workload and AWS's own prices.

Prices: the public AWS Price List bulk files for eu-west-2 (London), no account
needed. `--refresh-prices` re-downloads them and stores the handful of unit
prices used here in infra/aws_prices_eu-west-2.json (with publication dates);
otherwise that committed file is used, so the estimate is reproducible.

Workload: measured on this machine. The handler runs in a separate process
(only pandas, pyarrow and boto3 loaded, as on Lambda) against the local S3
emulator, three times on the real monthly file. We record duration, peak
memory and the S3 requests it makes (counted with a botocore hook).

Writes results/COSTS.md.

Usage: python scripts/estimate_costs.py [--refresh-prices]
"""
import argparse
import json
import math
import os
import statistics
import subprocess
from collections import Counter
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

PRICES = ROOT / "infra" / "aws_prices_eu-west-2.json"
RAW = ROOT / "data" / "raw" / "pp-monthly-update.csv"
META = ROOT / "data" / "raw" / "source.json"
OUT = ROOT / "results" / "COSTS.md"
REGION = "eu-west-2"
PRICE_LIST = "https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/{service}/current/eu-west-2/index.json"
COMPLETE_FILE = "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv"

# Assumptions (stated in the report).
RUNS = 3
MEMORY_MB = 1024           # template default (infra/template.yaml)
DURATION_SAFETY = 3        # Lambda arm64 at 1 GB gets ~0.6 vCPU; assume 3x this machine's time
RELEASES_KEPT = 12         # steady state: a year of monthly releases in both buckets
QUERIES_PER_MONTH = 500    # a small team's dashboards plus ad-hoc questions
ATHENA_MIN_MB = 10         # Athena bills at least 10 MB per query, rounded up to the MB
MB, GB, TB = 1024 ** 2, 1024 ** 3, 1024 ** 4

# (service, usagetype, description must contain) -> key
WANTED = {
    "lambda_gb_second_arm": ("AWSLambda", "EUW2-Lambda-GB-Second-ARM", "Tier-1"),
    "lambda_request_arm": ("AWSLambda", "EUW2-Request-ARM", ""),
    "lambda_free_gb_seconds": ("AWSLambda", "Global-Lambda-GB-Second", "Free Tier"),
    "lambda_free_requests": ("AWSLambda", "Global-Request", "Free Tier"),
    "s3_standard_gb_month": ("AmazonS3", "EUW2-TimedStorage-ByteHrs", "first 50 TB"),
    "s3_put_list_request": ("AmazonS3", "EUW2-Requests-Tier1", ""),
    "s3_get_request": ("AmazonS3", "EUW2-Requests-Tier2", ""),
    "athena_per_tb_scanned": ("AmazonAthena", "EUW2-DataScannedInTB", ""),
}
TIER1_OPS = {"PutObject", "CreateMultipartUpload", "UploadPart", "CompleteMultipartUpload",
             "CopyObject", "ListObjectsV2", "ListObjects"}


def refresh_prices() -> None:
    lists = {}
    for service in {s for s, _, _ in WANTED.values()}:
        with urllib.request.urlopen(PRICE_LIST.format(service=service), timeout=60) as resp:
            lists[service] = json.load(resp)
    prices = {"region": REGION, "sources": {}, "unit_prices_usd": {}}
    for key, (service, usagetype, must_contain) in WANTED.items():
        doc = lists[service]
        prices["sources"][service] = {"url": PRICE_LIST.format(service=service),
                                      "publication_date": doc["publicationDate"]}
        hits = []
        for sku, product in doc["products"].items():
            if product["attributes"].get("usagetype") != usagetype:
                continue
            for term in doc["terms"]["OnDemand"].get(sku, {}).values():
                for dim in term["priceDimensions"].values():
                    if must_contain in dim["description"]:
                        hits.append({"usd": float(dim["pricePerUnit"]["USD"]), "unit": dim["unit"],
                                     "description": dim["description"], "end_range": dim["endRange"]})
        if len(hits) != 1:
            raise SystemExit(f"{key}: expected 1 price, found {len(hits)}")
        prices["unit_prices_usd"][key] = hits[0]
    req = urllib.request.Request(COMPLETE_FILE, method="HEAD")
    with urllib.request.urlopen(req, timeout=60) as resp:
        prices["backfill_source"] = {"url": COMPLETE_FILE, "bytes": int(resp.headers["Content-Length"]),
                                     "last_modified": resp.headers["Last-Modified"]}
    PRICES.write_text(json.dumps(prices, indent=2) + "\n")
    print(f"wrote {PRICES.relative_to(ROOT)}")


def child(key: str) -> None:
    """Runs in a fresh process: one processing run, then one duplicate event."""
    import resource
    import time

    t0 = time.perf_counter()
    import boto3
    from lake.handler import lambda_handler
    from lake import config
    from lake.infra import s3_put_event
    import_s = time.perf_counter() - t0

    s3 = boto3.client("s3")
    calls: list[str] = []
    s3.meta.events.register("before-call.s3", lambda model, **_: calls.append(model.name))
    event = s3_put_event(config.landing_bucket(), key, "unused", 0, REGION)
    t1 = time.perf_counter()
    first = lambda_handler(event, s3=s3)["results"][0]
    handler_s = time.perf_counter() - t1
    first_calls, calls[:] = list(calls), []
    t2 = time.perf_counter()
    second = lambda_handler(event, s3=s3)["results"][0]
    duplicate_s = time.perf_counter() - t2
    print(json.dumps({
        "import_s": import_s, "handler_s": handler_s, "duplicate_s": duplicate_s,
        "peak_rss_mb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024,
        "status": [first["status"], second["status"]],
        "calls_processed": first_calls, "calls_duplicate": list(calls),
        "manifest": first,
    }))


def measure() -> tuple[list[dict], list[str]]:
    import boto3
    from lake import config
    from lake.infra import ensure_buckets, local_aws

    release = json.loads(META.read_text())["release"]
    key = config.landing_key(release)
    runs = []
    with local_aws(region=REGION):
        s3 = boto3.client("s3")
        ensure_buckets(s3, REGION)
        upload_calls: list[str] = []
        s3.meta.events.register("before-call.s3", lambda model, **_: upload_calls.append(model.name))
        s3.upload_file(str(RAW), config.landing_bucket(), key)
        for _ in range(RUNS):
            s3.delete_object(Bucket=config.lake_bucket(), Key=config.manifest_key(release))
            out = subprocess.run([sys.executable, __file__, "--child", key], env=os.environ.copy(),
                                 capture_output=True, text=True, check=True).stdout
            runs.append(json.loads(out.strip().splitlines()[-1]))
    upload_calls = [c for c in upload_calls if c != "DeleteObject"]
    return runs, upload_calls


def requests(calls: list[str]) -> tuple[int, int]:
    tier1 = sum(c in TIER1_OPS for c in calls)
    return tier1, len(calls) - tier1


def athena_cost(scan_bytes: float, queries: int, per_tb: float) -> float:
    billed_mb = max(ATHENA_MIN_MB, math.ceil(scan_bytes / MB))
    return queries * billed_mb * MB / TB * per_tb


def estimate(prices: dict, runs: list[dict], upload_calls: list[str]) -> dict:
    p = {k: v["usd"] for k, v in prices["unit_prices_usd"].items()}
    m = runs[0]["manifest"]
    lambda_s = statistics.median(r["import_s"] + r["handler_s"] for r in runs)
    peak_mb = max(r["peak_rss_mb"] for r in runs)
    billed_s = lambda_s * DURATION_SAFETY
    invocations = 2  # the release plus one duplicate delivery
    duplicate_s = statistics.median(r["duplicate_s"] for r in runs) * DURATION_SAFETY

    gb_s = (billed_s + duplicate_s) * MEMORY_MB / 1024
    lake_bytes = m["outputs"]["curated_bytes"] + 1_000  # + manifest and empty quarantine file
    stored_gb = RELEASES_KEPT * (m["source_bytes"] + lake_bytes) / GB
    up1, up2 = requests(upload_calls)
    p1, p2 = requests(runs[0]["calls_processed"])
    d1, d2 = requests(runs[0]["calls_duplicate"])
    tier1, tier2 = up1 + p1 + d1, up2 + p2 + d2

    lake_scan = RELEASES_KEPT * m["outputs"]["curated_bytes"]
    csv_scan = RELEASES_KEPT * m["source_bytes"]
    lines = {
        "Lambda compute": gb_s * p["lambda_gb_second_arm"],
        "Lambda requests": invocations * p["lambda_request_arm"],
        "S3 storage (12 releases, both buckets)": stored_gb * p["s3_standard_gb_month"],
        "S3 PUT/LIST requests": tier1 * p["s3_put_list_request"],
        "S3 GET requests (handler)": tier2 * p["s3_get_request"],
        f"Athena ({QUERIES_PER_MONTH} queries over the Parquet lake)":
            athena_cost(lake_scan, QUERIES_PER_MONTH, p["athena_per_tb_scanned"]),
    }
    backfill = prices["backfill_source"]
    est_rows = backfill["bytes"] / (m["source_bytes"] / m["rows_in"])
    est_parquet = est_rows * m["outputs"]["curated_bytes"] / m["rows_valid"]
    return {
        "lambda_s": lambda_s, "peak_mb": peak_mb, "billed_s": billed_s, "gb_s": gb_s,
        "runs": runs, "upload_calls": upload_calls, "tier1": tier1, "tier2": tier2,
        "stored_gb": stored_gb, "lines": lines, "total": sum(lines.values()),
        "lake_scan": lake_scan, "csv_scan": csv_scan,
        "athena_lake": lines[f"Athena ({QUERIES_PER_MONTH} queries over the Parquet lake)"],
        "athena_csv": athena_cost(csv_scan, QUERIES_PER_MONTH, p["athena_per_tb_scanned"]),
        "x10_lambda": gb_s * 10 * p["lambda_gb_second_arm"],
        "x10_queries": athena_cost(lake_scan, QUERIES_PER_MONTH * 10, p["athena_per_tb_scanned"]),
        "free_gb_s": p.get("lambda_free_gb_seconds"),
        "backfill_rows": est_rows, "backfill_parquet": est_parquet,
        "backfill_storage": (backfill["bytes"] + est_parquet) / GB * p["s3_standard_gb_month"],
        "backfill_athena": athena_cost(est_parquet, QUERIES_PER_MONTH, p["athena_per_tb_scanned"]),
    }


def tally(calls: list[str]) -> str:
    return ", ".join(f"{n} {op}" for op, n in Counter(calls).items())


def usd(x: float) -> str:
    if x < 0.0001:
        return "< $0.0001"
    return f"${x:,.4f}" if x < 1 else f"${x:,.2f}"


def write_report(prices: dict, e: dict) -> None:
    up = prices["unit_prices_usd"]
    m = e["runs"][0]["manifest"]
    free = prices["unit_prices_usd"]["lambda_free_gb_seconds"]
    calls = e["runs"][0]
    rows = [
        "# Running cost estimate (AWS, eu-west-2 London)",
        "",
        "_Generated by `scripts/estimate_costs.py`. Prices come from the public AWS Price List; "
        "the workload is measured on this machine against the local S3 emulator._",
        "",
        "## Unit prices used",
        "| Item | Price (USD) | AWS description |",
        "|---|---:|---|",
    ]
    for key, v in up.items():
        if "free" in key:
            continue
        rows.append(f"| `{key}` | ${v['usd']:.10f}".rstrip("0").rstrip(".") + f" per {v['unit']} | {v['description']} |")
    rows += ["", "Sources (publication date):"]
    for service, src in sorted(prices["sources"].items()):
        rows.append(f"- {service}: {src['url']} ({src['publication_date'][:10]})")
    rows += [
        "- Athena's 10 MB minimum per query: https://aws.amazon.com/athena/pricing/",
        "",
        "## Measured workload (one monthly release)",
        f"- File: {m['source_bytes']:,} bytes, {m['rows_in']:,} rows → Parquet {m['outputs']['curated_bytes']:,} bytes",
        f"- Handler, {RUNS} runs in a fresh process (import + processing), median: "
        f"**{e['lambda_s']:.2f} s**; runs: "
        + ", ".join(f"{r['import_s'] + r['handler_s']:.2f} s" for r in e["runs"]),
        f"- Peak memory (max RSS of that process): **{e['peak_mb']:.0f} MB**, "
        f"{e['peak_mb'] / MEMORY_MB:.0%} of the {MEMORY_MB} MB set in the template",
        f"- Duplicate event: {calls['status'][1]} in "
        f"{statistics.median(r['duplicate_s'] for r in e['runs']):.3f} s (median)",
        f"- S3 calls by the handler: processing {tally(calls['calls_processed'])}; "
        f"duplicate {tally(calls['calls_duplicate'])}",
        f"- S3 calls to upload the file (boto3 multipart above 8 MB): {tally(e['upload_calls'])}",
        "",
        "## Monthly cost, steady state",
        f"Assumptions: Lambda billed at **{DURATION_SAFETY}× this machine's time** "
        f"({e['billed_s']:.1f} s) at {MEMORY_MB} MB on arm64, plus one duplicate delivery a month; "
        f"{RELEASES_KEPT} releases kept in both buckets; "
        f"{QUERIES_PER_MONTH} Athena queries a month, each assumed to scan the whole lake "
        f"({e['lake_scan'] / MB:.1f} MB; a real query reads fewer columns and partitions). "
        "Prices are before any free tier.",
        "",
        "| Line item | USD / month |",
        "|---|---:|",
    ]
    rows += [f"| {k} | {usd(v)} |" for k, v in e["lines"].items()]
    rows += [
        f"| **Total** | **{usd(e['total'])}** |",
        "",
        f"Lambda use is {e['gb_s']:.0f} GB-seconds a month. The price list's always-free Lambda "
        f"allowance is {int(free['end_range']):,} GB-seconds, so on most accounts the compute line would be $0.",
        "",
        "## What moves the bill",
        "| Scenario | USD / month |",
        "|---|---:|",
        f"| Lambda runs 10× longer than assumed ({DURATION_SAFETY * 10}× this machine) | {usd(e['x10_lambda'])} (Lambda compute) |",
        f"| 10× the queries ({QUERIES_PER_MONTH * 10:,}/month) on the Parquet lake | {usd(e['x10_queries'])} (Athena) |",
        f"| Same {QUERIES_PER_MONTH} queries on the raw CSVs instead of Parquet "
        f"({e['csv_scan'] / MB:.0f} MB per full scan) | {usd(e['athena_csv'])} (Athena) |",
        "",
        f"Querying Parquet instead of the raw CSV cuts the Athena line by "
        f"{1 - e['athena_lake'] / e['athena_csv']:.0%} at the same query volume.",
        "",
        "## Historical backfill (illustrative)",
        f"HM Land Registry's complete file is {prices['backfill_source']['bytes'] / GB:.2f} GB "
        f"(HEAD request, Last-Modified {prices['backfill_source']['last_modified']}). "
        f"At this release's {m['source_bytes'] / m['rows_in']:.0f} bytes per CSV row that is about "
        f"{e['backfill_rows'] / 1e6:.1f} million rows, and at {m['outputs']['curated_bytes'] / m['rows_valid']:.1f} "
        f"bytes per Parquet row about {e['backfill_parquet'] / GB:.2f} GB of Parquet. Both ratios are scaled "
        "linearly from one monthly file, so treat them as rough.",
        "",
        f"- Storing the raw file plus its Parquet: about {usd(e['backfill_storage'])} a month.",
        f"- {QUERIES_PER_MONTH} queries a month that each scanned all history: about {usd(e['backfill_athena'])}. "
        "Partitioning by year and selecting few columns would cut this a lot.",
        "- The handler reads a whole file into memory, and Lambda allows at most 10 GB of memory and 15 minutes, "
        f"so the {prices['backfill_source']['bytes'] / GB:.2f} GB complete file is far too big for one run. "
        "A backfill should load the yearly files instead (see docs/DEPLOY.md).",
        "",
    ]
    OUT.write_text("\n".join(rows))
    print(f"wrote {OUT.relative_to(ROOT)}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--refresh-prices", action="store_true")
    parser.add_argument("--child", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.child:
        return child(args.child)
    if args.refresh_prices or not PRICES.exists():
        refresh_prices()
    if not RAW.exists():
        sys.exit("raw file missing: run `python scripts/download_data.py` first")
    prices = json.loads(PRICES.read_text())
    runs, upload_calls = measure()
    write_report(prices, estimate(prices, runs, upload_calls))


if __name__ == "__main__":
    main()
