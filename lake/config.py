"""Bucket names and lake layout.

Bucket names come from environment variables (read at call time), so the same
code runs against real AWS (Lambda environment variables) or the local moto
emulator.
"""
import os

# Landing key layout: price-paid/monthly/release=YYYY-MM/<file>.csv
LANDING_PREFIX = "price-paid/monthly/"

CURATED_PREFIX = "curated/price_paid_changes/"
QUARANTINE_PREFIX = "quarantine/price_paid/"
MANIFEST_PREFIX = "_manifests/price_paid/"


def landing_bucket() -> str:
    return os.environ.get("LANDING_BUCKET", "ppd-landing-demo")


def lake_bucket() -> str:
    return os.environ.get("LAKE_BUCKET", "ppd-lake-demo")


def landing_key(release: str, filename: str = "pp-monthly-update.csv") -> str:
    return f"{LANDING_PREFIX}release={release}/{filename}"


def curated_key(release: str) -> str:
    return f"{CURATED_PREFIX}release={release}/part-000.parquet"


def quarantine_key(release: str) -> str:
    return f"{QUARANTINE_PREFIX}release={release}/rejected.csv"


def manifest_key(release: str) -> str:
    return f"{MANIFEST_PREFIX}release={release}.json"
