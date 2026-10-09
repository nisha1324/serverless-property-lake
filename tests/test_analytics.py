"""Offline tests for the SQL in sql/: a tiny two-release lake written as local
Parquet in the same release=YYYY-MM layout as S3."""
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from lake.query import register_changes, run_sql_dir
from lake.schema import CURATED_COLUMNS

SQL_DIR = Path(__file__).resolve().parents[1] / "sql"


def rec(tid, release, status, price, transfer, new_build=False, ptype="S",
        district="NW7", cat="A") -> dict:
    return {
        "transaction_id": tid, "price": price, "date_of_transfer": transfer,
        "transfer_month": transfer.strftime("%Y-%m"), "postcode_district": district,
        "postcode_sector": f"{district} 1", "property_type": ptype, "new_build": new_build,
        "tenure": "F", "town_city": "LONDON", "district": "BARNET",
        "county": "GREATER LONDON", "ppd_category": cat, "record_status": status,
        "release": release,
    }


RELEASES = {
    "2026-08": [
        rec("t1", "2026-08", "A", 200_000, date(2026, 6, 15)),
        rec("t2", "2026-08", "A", 300_000, date(2026, 6, 20)),
        rec("t3", "2026-08", "A", 400_000, date(2026, 7, 1)),
    ],
    "2026-09": [
        rec("t1", "2026-09", "C", 210_000, date(2026, 6, 15)),   # price corrected
        rec("t2", "2026-09", "D", 300_000, date(2026, 6, 20)),   # withdrawn
        rec("t4", "2026-09", "A", 500_000, date(2025, 10, 3), new_build=True),
        rec("t5", "2026-09", "C", 250_000, date(2024, 2, 9)),    # history not in lake
        rec("t6", "2026-09", "A", 150_000, date(2026, 8, 28)),
    ],
}


@pytest.fixture
def con(tmp_path):
    for release, rows in RELEASES.items():
        part = tmp_path / f"release={release}"
        part.mkdir()
        # Like the handler's output, `release` is also stored inside the file.
        pd.DataFrame(rows)[CURATED_COLUMNS].to_parquet(part / "part-000.parquet", index=False)
    c = duckdb.connect()
    register_changes(c, f"{tmp_path}/*/*.parquet")
    return c


def test_current_view_applies_changes_and_deletions(con):
    results = run_sql_dir(con, SQL_DIR)
    assert "01_current_view" not in results  # DDL only, nothing to report
    current = con.sql("SELECT transaction_id, price FROM price_paid_current ORDER BY 1").df()
    assert current["transaction_id"].tolist() == ["t1", "t3", "t4", "t5", "t6"]
    assert current.set_index("transaction_id").loc["t1", "price"] == 210_000


def test_change_feed_flags_changes_with_no_history(con):
    feed = run_sql_dir(con, SQL_DIR)["02_change_feed"].set_index(["release", "record_status"])
    assert feed.loc[("2026-09", "C"), "rows"] == 2
    assert feed.loc[("2026-09", "C"), "matches_earlier_release"] == 1   # t1
    assert feed.loc[("2026-09", "C"), "no_earlier_record"] == 1         # t5
    assert feed.loc[("2026-09", "D"), "matches_earlier_release"] == 1   # t2
    assert feed.loc[("2026-08", "A"), "no_earlier_record"] == 3


def test_registration_lag_uses_latest_release_added_rows(con):
    lag = run_sql_dir(con, SQL_DIR)["03_registration_lag"]
    rows = {(r.segment, r.lag_bucket): (r.sales, r.pct_of_segment) for r in lag.itertuples()}
    # Only t4 (Oct 2025 → Sep 2026 = 11 months) and t6 (Aug → Sep = 1 month) are A rows in 2026-09.
    assert rows == {("existing", "1 month"): (1, 100.0), ("new build", "7-12 months"): (1, 100.0)}


def test_market_window_excludes_old_transfers_and_deleted_sales(con):
    market = run_sql_dir(con, SQL_DIR)["04_market_by_type"]
    semi = market.set_index("property_type").loc["Semi-detached"]
    # 12 months before Sep 2026 = Sep 2025..Aug 2026: t1, t3, t4, t6 (t2 deleted, t5 from 2024).
    assert semi["standard_sales"] == 4
    assert semi["median_price"] == 305_000
