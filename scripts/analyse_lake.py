"""SQL analytics over the lake, queried in place on (emulated) S3.

1. start the moto S3 emulator and ingest the monthly file through the Lambda
   handler, as in `run_local.py` (the emulator is in-memory, so each run
   rebuilds the lake);
2. point DuckDB at the curated Parquet in S3 and run every query in `sql/`;
3. write `results/ANALYTICS.md`, one CSV per query in `results/csv/` and the
   charts in `results/charts/`.

Usage: python scripts/download_data.py && python scripts/analyse_lake.py
"""
import json
import sys
from pathlib import Path

import boto3
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from lake import config  # noqa: E402
from lake.handler import lambda_handler  # noqa: E402
from lake.infra import ensure_buckets, local_aws, s3_put_event  # noqa: E402
from lake.query import connect_s3, lake_glob, register_changes, run_sql_dir  # noqa: E402

RAW = ROOT / "data" / "raw" / "pp-monthly-update.csv"
META = ROOT / "data" / "raw" / "source.json"
SQL_DIR = ROOT / "sql"
RESULTS = ROOT / "results"
CHARTS = RESULTS / "charts"
CSV_DIR = RESULTS / "csv"
OUT = RESULTS / "ANALYTICS.md"
REGION = "eu-west-2"

BLUE = "#2a78d6"     # categorical slot 1
ORANGE = "#eb6834"   # categorical slot 2
INK = "#0b0b0b"
INK_2 = "#52514e"
MUTED = "#898781"
GRID = "#e8e7e3"


def build_lake_and_query() -> dict[str, pd.DataFrame]:
    source = json.loads(META.read_text())
    release = source["release"]
    with local_aws(region=REGION) as endpoint:
        s3 = boto3.client("s3")
        ensure_buckets(s3, REGION)
        key = config.landing_key(release)
        s3.upload_file(str(RAW), config.landing_bucket(), key)
        head = s3.head_object(Bucket=config.landing_bucket(), Key=key)
        lambda_handler(s3_put_event(config.landing_bucket(), key, head["ETag"].strip('"'),
                                    head["ContentLength"], REGION))
        con = connect_s3(endpoint, REGION)
        register_changes(con, lake_glob())
        return run_sql_dir(con, SQL_DIR)


def _style(ax, grid_axis: str) -> None:
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(MUTED)
    ax.tick_params(colors=INK_2, labelsize=9)
    ax.grid(axis=grid_axis, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)


def _save(fig, name: str) -> Path:
    fig.tight_layout()
    path = CHARTS / name
    fig.savefig(path, dpi=150, facecolor="#fcfcfb")
    plt.close(fig)
    return path


def chart_lag(df: pd.DataFrame) -> Path:
    order = (df.drop_duplicates("lag_bucket").sort_values("sort_key")["lag_bucket"].tolist())
    wide = df.pivot(index="lag_bucket", columns="segment", values="pct_of_segment").reindex(order).fillna(0)
    sales = df.groupby("segment")["sales"].sum()
    fig, ax = plt.subplots(figsize=(8.5, 4))
    x = range(len(order))
    w = 0.38
    for i, (seg, color) in enumerate([("existing", BLUE), ("new build", ORANGE)]):
        xs = [v + (i - 0.5) * (w + 0.02) for v in x]
        bars = ax.bar(xs, wide[seg], width=w, color=color,
                      label=f"{seg.capitalize()} homes ({sales[seg]:,} sales)")
        for bar, val in zip(bars, wide[seg]):
            if val >= 10:
                ax.text(bar.get_x() + bar.get_width() / 2, val + 1, f"{val:.0f}%",
                        ha="center", color=INK_2, fontsize=8.5)
    ax.set_xticks(list(x), order)
    ax.set_xlabel("Time between the sale completing and the release that published it", color=INK_2)
    ax.set_ylabel("% of the segment's added sales", color=INK_2)
    ax.set_ylim(0, max(wide.max()) + 8)
    ax.legend(frameon=False, fontsize=9, loc="upper left")
    ax.set_title("New-build sales reach the register months later than existing homes\n"
                 "Added sales in the September 2026 Price Paid release",
                 loc="left", color=INK, fontsize=11)
    _style(ax, "y")
    return _save(fig, "01_registration_lag.png")


def chart_counties(df: pd.DataFrame, median_all: float) -> Path:
    d = df.sort_values("median_price")
    labels = d["county"].str.title()
    fig, ax = plt.subplots(figsize=(8.5, 5.2))
    ax.barh(labels, d["median_price"] / 1000, color=BLUE, height=0.62)
    for y, (p, n) in enumerate(zip(d["median_price"], d["sales"])):
        ax.text(p / 1000 + 6, y, f"£{p / 1000:,.0f}k  ({n:,} sales)", va="center", color=INK_2, fontsize=8.5,
                bbox={"facecolor": "#fcfcfb", "edgecolor": "none", "pad": 0.6}, zorder=3)
    ax.axvline(median_all / 1000, color=INK_2, linewidth=1, linestyle="--", zorder=2)
    ax.text(median_all / 1000 + 4, len(d) - 0.4, f"England & Wales median £{median_all / 1000:,.0f}k",
            color=INK_2, fontsize=8.5, va="bottom")
    ax.set_xlim(0, d["median_price"].max() / 1000 * 1.35)
    ax.set_xlabel("Median standard sale price (£k)", color=INK_2)
    ax.set_title("Median sale price in the 15 busiest counties\n"
                 "Standard sales in the 12 months to Aug 2026, as registered in the Sep 2026 release",
                 loc="left", color=INK, fontsize=11)
    _style(ax, "x")
    return _save(fig, "02_county_prices.png")


def chart_premium(df: pd.DataFrame) -> Path:
    d = df.sort_values("median_premium_pct")
    fig, ax = plt.subplots(figsize=(8.5, 3.6))
    y = range(len(d))
    ax.hlines(list(y), d["p25_premium_pct"], d["p75_premium_pct"], color=BLUE, alpha=0.35, linewidth=7)
    ax.plot(d["median_premium_pct"], list(y), "o", color=BLUE, markersize=9)
    for yy, (m, n, c) in enumerate(zip(d["median_premium_pct"], d["new_sales"], d["districts"])):
        ax.text(d["p75_premium_pct"].iloc[yy] + 2, yy, f"median {m:+.0f}%  ({c} districts, {n:,} new sales)",
                va="center", color=INK_2, fontsize=8.5)
    ax.axvline(0, color=MUTED, linewidth=1)
    ax.set_yticks(list(y), d["property_type"])
    ax.set_xlim(min(-20, d["p25_premium_pct"].min() - 5), d["p75_premium_pct"].max() + 75)
    ax.set_xlabel("New-build median vs existing median, same postcode district and type (%)\n"
                  "dot = median across districts, band = middle 50% of districts", color=INK_2)
    ax.set_title("New-build premium, compared like for like\n"
                 "Standard sales in the 12 months to Aug 2026, districts with 5+ new and 5+ existing sales",
                 loc="left", color=INK, fontsize=11)
    _style(ax, "x")
    return _save(fig, "03_new_build_premium.png")


def md(df: pd.DataFrame) -> str:
    """Markdown table: thousands separators, whole numbers without decimals,
    percentages to 1 dp, and '-' for empty cells."""
    out = df.copy()
    for col in out.select_dtypes("number"):
        whole = (out[col].dropna() % 1 == 0).all()
        fmt = "{:,.0f}" if whole else "{:,.1f}"
        out[col] = out[col].map(lambda v: "-" if pd.isna(v) else fmt.format(v))
    numeric = [c in df.select_dtypes("number").columns for c in out.columns]
    return out.to_markdown(index=False, disable_numparse=True,
                           colalign=["right" if n else "left" for n in numeric])


def write_report(r: dict[str, pd.DataFrame], median_all: float, release: str) -> None:
    feed = r["02_change_feed"]
    lag = r["03_registration_lag"].drop(columns="sort_key")
    lines = [
        f"# Lake analytics: release {release}",
        "",
        "_Generated by `scripts/analyse_lake.py`: DuckDB querying the curated Parquet in place on the "
        "local S3 emulator. Queries are in [`sql/`](../sql); one CSV per query in [`csv/`](csv)._",
        "",
        "## 1. Applying the change feed (`01_current_view`, `02_change_feed`)",
        "`price_paid_current` keeps the latest version of each transaction across releases and drops deletions.",
        "",
        md(feed),
        "",
        "A C or D row with no earlier record means the lake doesn't yet hold the sale being corrected or deleted.",
        "",
        "## 2. Registration lag (`03_registration_lag`)",
        "Added sales in the latest release, by months between the transfer and the release month.",
        "",
        md(lag),
        "",
        "![Registration lag](charts/01_registration_lag.png)",
        "",
        "## 3. Market by property type (`04_market_by_type`)",
        "Current view, transfers in the 12 months before the release month. Prices use standard (category A) "
        "sales only; `pct_additional` is the share of category B entries (repossessions, buy-to-let, company sales).",
        "",
        md(r["04_market_by_type"]),
        "",
        "## 4. Busiest counties (`05_counties`)",
        f"Standard sales, same window. Price index: England & Wales median (£{median_all:,.0f}) = 100.",
        "",
        md(r["05_counties"].drop(columns="england_wales_median")),
        "",
        "![County prices](charts/02_county_prices.png)",
        "",
        "## 5. New-build premium (`06_new_build_premium`)",
        "Per postcode district × property type with at least 5 new and 5 existing standard sales.",
        "",
        md(r["06_new_build_premium"]),
        "",
        "![New-build premium](charts/03_new_build_premium.png)",
        "",
    ]
    OUT.write_text("\n".join(lines))


def main() -> None:
    if not RAW.exists() or not META.exists():
        sys.exit("raw file missing: run `python scripts/download_data.py` first")
    release = json.loads(META.read_text())["release"]
    results = build_lake_and_query()
    CHARTS.mkdir(parents=True, exist_ok=True)
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    for name, df in results.items():
        df.to_csv(CSV_DIR / f"{name}.csv", index=False)

    counties = results["05_counties"]
    median_all = float(counties["england_wales_median"].iloc[0])
    chart_lag(results["03_registration_lag"])
    chart_counties(counties, median_all)
    chart_premium(results["06_new_build_premium"])
    write_report(results, median_all, release)
    print(f"wrote {OUT.relative_to(ROOT)}, {len(results)} CSVs and 3 charts")


if __name__ == "__main__":
    main()
