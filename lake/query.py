"""Query the lake in place with DuckDB, the way Athena would on AWS.

`connect_s3` points DuckDB at an S3 endpoint (the local moto emulator here);
`register_changes` exposes every curated release as one `changes` view; and
`run_sql_dir` runs the numbered files in `sql/` against it. The SQL only ever
refers to `changes`, so the same queries run over S3 or over local Parquet
files in tests.
"""
import re
from pathlib import Path

import duckdb
import pandas as pd

from lake import config

DDL = re.compile(r"^\s*CREATE\s", re.IGNORECASE | re.MULTILINE)


def connect_s3(endpoint: str, region: str) -> duckdb.DuckDBPyConnection:
    """DuckDB connection that reads s3:// paths from a local S3-compatible endpoint."""
    con = duckdb.connect()
    con.execute("LOAD httpfs")
    con.execute(f"""CREATE SECRET (TYPE S3, KEY_ID 'local-test', SECRET 'local-test',
        REGION '{region}', ENDPOINT '{endpoint}', URL_STYLE 'path', USE_SSL false)""")
    return con


def lake_glob() -> str:
    return f"s3://{config.lake_bucket()}/{config.CURATED_PREFIX}*/*.parquet"


def register_changes(con: duckdb.DuckDBPyConnection, glob: str) -> None:
    """One view over all release partitions. hive_partitioning lets a filter on
    `release` skip whole folders (partition pruning)."""
    con.execute(f"""CREATE OR REPLACE VIEW changes AS
        SELECT * FROM read_parquet('{glob}', hive_partitioning = true)""")


def run_sql_dir(con: duckdb.DuckDBPyConnection, sql_dir: Path) -> dict[str, pd.DataFrame]:
    """Run each .sql file in name order. Files that return rows are collected
    by file stem; DDL files (e.g. CREATE VIEW) just run."""
    out = {}
    for path in sorted(sql_dir.glob("*.sql")):
        sql = path.read_text()
        rel = con.execute(sql)
        if not DDL.search(sql):
            out[path.stem] = rel.df()
    return out
