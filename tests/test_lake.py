"""Offline tests: validation rules, curated shape, and the handler end to end
against moto's in-process S3 mock (no network, no AWS account)."""
import io
import json

import boto3
import pandas as pd
import pyarrow.parquet as pq
import pytest
from moto import mock_aws

from lake import config
from lake.handler import lambda_handler
from lake.infra import ensure_buckets, s3_put_event
from lake.schema import RAW_COLUMNS, read_raw, to_curated, validate

REGION = "eu-west-2"
RELEASE = "2026-09"
GOOD_ID = "{5A7B802C-4C8F-1886-E063-4704A8C021AE}"


def row(tid=GOOD_ID, price="428000", date="2021-07-31 00:00", postcode="NW7 1SN",
        ptype="F", new="Y", tenure="L", cat="A", status="A") -> list[str]:
    return [tid, price, date, postcode, ptype, new, tenure, "BARTRAM HOUSE, 10", "FLAT 29",
            "MAURICE BROWNE AVENUE", "", "LONDON", "BARNET", "GREATER LONDON", cat, status]


def gid(n: int) -> str:
    return "{5A7B802C-0000-1886-E063-%012X}" % n


def to_csv(rows: list[list[str]]) -> bytes:
    return pd.DataFrame(rows).to_csv(header=False, index=False, quoting=1).encode()


SAMPLE = [
    row(gid(1)),
    row(gid(2), postcode="", ptype="D", new="N", tenure="F", status="C"),
    row(gid(3), price="100", status="D"),
    row("not-a-guid"),                                  # bad_transaction_id
    row(gid(1)),                                        # duplicate_id
    row(gid(4), price="12.5"),                          # bad_price
    row(gid(5), price="0"),                             # bad_price
    row(gid(6), date="31/07/2021"),                     # bad_date
    row(gid(7), date="2026-10-01 00:00"),               # date_after_release
    row(gid(8), ptype="X"),                             # bad_property_type
    row(gid(9), status="Z"),                            # bad_record_status
]


def test_validate_tags_first_broken_rule():
    valid, rejected = validate(read_raw(to_csv(SAMPLE)), RELEASE)
    assert len(valid) == 3
    assert rejected["reject_reason"].tolist() == [
        "bad_transaction_id", "duplicate_id", "bad_price", "bad_price", "bad_date",
        "date_after_release", "bad_property_type", "bad_record_status"]
    # A row breaking two rules is tagged with the first only.
    _, both = validate(read_raw(to_csv([row("bad", price="-1")])), RELEASE)
    assert both["reject_reason"].tolist() == ["bad_transaction_id"]


def test_curated_drops_address_and_derives_postcode_parts():
    valid, _ = validate(read_raw(to_csv(SAMPLE[:3])), RELEASE)
    cur = to_curated(valid, RELEASE)
    assert not {"paon", "saon", "street", "locality", "postcode"} & set(cur.columns)
    assert cur.loc[0, "postcode_district"] == "NW7"
    assert cur.loc[0, "postcode_sector"] == "NW7 1"
    assert pd.isna(cur.loc[1, "postcode_district"])
    assert cur["new_build"].tolist() == [True, False, True]
    assert cur["transfer_month"].tolist() == ["2021-07"] * 3
    assert cur["price"].dtype == "int64"


@pytest.fixture
def s3(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    with mock_aws():
        client = boto3.client("s3", region_name=REGION)
        ensure_buckets(client, REGION)
        yield client


def land(s3, body: bytes, release: str = RELEASE) -> dict:
    key = config.landing_key(release)
    s3.put_object(Bucket=config.landing_bucket(), Key=key, Body=body)
    head = s3.head_object(Bucket=config.landing_bucket(), Key=key)
    return s3_put_event(config.landing_bucket(), key, head["ETag"].strip('"'), len(body), REGION)


def read_curated(s3, release: str = RELEASE) -> pd.DataFrame:
    obj = s3.get_object(Bucket=config.lake_bucket(), Key=config.curated_key(release))
    return pq.read_table(io.BytesIO(obj["Body"].read())).to_pandas()


def test_handler_writes_curated_quarantine_and_manifest(s3):
    event = land(s3, to_csv(SAMPLE))
    # Real S3 events URL-encode the key ("=" → "%3D"); the handler must decode it.
    assert "release%3D2026-09" in event["Records"][0]["s3"]["object"]["key"]
    result = lambda_handler(event, s3=s3)["results"][0]

    assert result["status"] == "processed"
    assert (result["rows_in"], result["rows_valid"], result["rows_rejected"]) == (11, 3, 8)
    assert result["rejected_by_reason"]["bad_price"] == 2
    assert result["record_status"] == {"A": 1, "C": 1, "D": 1}
    assert result["quality_flags"]["postcode_missing_or_invalid"] == 1
    assert result["quality_flags"]["price_below_10k"] == 1

    assert len(read_curated(s3)) == 3
    quarantine = s3.get_object(Bucket=config.lake_bucket(), Key=config.quarantine_key(RELEASE))
    assert len(pd.read_csv(quarantine["Body"])) == 8
    manifest = json.loads(s3.get_object(
        Bucket=config.lake_bucket(), Key=config.manifest_key(RELEASE))["Body"].read())
    assert manifest["source_etag"] == result["source_etag"]


def test_duplicate_event_skipped_and_reupload_overwrites(s3):
    event = land(s3, to_csv(SAMPLE))
    assert lambda_handler(event, s3=s3)["results"][0]["status"] == "processed"
    assert lambda_handler(event, s3=s3)["results"][0]["status"] == "skipped"

    # A corrected file for the same release has a new ETag: reprocess, same keys.
    fixed = land(s3, to_csv(SAMPLE[:2]))
    again = lambda_handler(fixed, s3=s3)["results"][0]
    assert again["status"] == "processed"
    assert again["previous_etag"] == event["Records"][0]["s3"]["object"]["eTag"]
    assert len(read_curated(s3)) == 2
    keys = [o["Key"] for o in s3.list_objects_v2(
        Bucket=config.lake_bucket(), Prefix=config.CURATED_PREFIX)["Contents"]]
    assert keys == [config.curated_key(RELEASE)]


def test_keys_outside_the_landing_layout_are_ignored(s3):
    s3.put_object(Bucket=config.landing_bucket(), Key="misc/notes.csv", Body=b"x")
    event = s3_put_event(config.landing_bucket(), "misc/notes.csv", "abc", 1, REGION)
    assert lambda_handler(event, s3=s3)["results"][0]["status"] == "ignored"


def test_bucket_defaults_applied_and_setup_is_idempotent(s3):
    assert ensure_buckets(s3, REGION) == []  # second run creates nothing
    for bucket in (config.landing_bucket(), config.lake_bucket()):
        block = s3.get_public_access_block(Bucket=bucket)["PublicAccessBlockConfiguration"]
        assert all(block.values())
        enc = s3.get_bucket_encryption(Bucket=bucket)["ServerSideEncryptionConfiguration"]
        assert enc["Rules"][0]["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"] == "AES256"
        assert s3.get_bucket_versioning(Bucket=bucket)["Status"] == "Enabled"
    rules = s3.get_bucket_lifecycle_configuration(Bucket=config.lake_bucket())["Rules"]
    assert rules[0]["Filter"]["Prefix"] == config.QUARANTINE_PREFIX
    assert rules[0]["Expiration"]["Days"] == 90


def test_raw_columns_match_published_field_count():
    assert len(RAW_COLUMNS) == 16
