"""AWS Lambda handler: S3 "object created" event → validated Parquet in the lake.

For each new monthly file in the landing bucket it:
1. reads the CSV and validates every row (`lake.schema.validate`);
2. writes valid rows, address-free, to the curated zone as Parquet,
   partitioned by release;
3. writes rejected rows, with the reason, to a quarantine CSV;
4. writes a JSON manifest last, so a release is only "published" once its
   data is fully written.

S3 delivers events *at least once*, so the handler is idempotent: if the
manifest for that release already records the same source ETag, the event is
skipped. A corrected re-upload (new ETag) reprocesses and overwrites the same
keys, so no duplicate rows appear.

boto3 picks up `AWS_ENDPOINT_URL` from the environment, which is how the same
code talks to the local moto emulator instead of AWS.
"""
import io
import json
import re
from datetime import datetime, timezone
from urllib.parse import unquote_plus

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from lake import config
from lake.schema import quality_flags, read_raw, to_curated, validate

RELEASE_IN_KEY = re.compile(r"release=(\d{4}-\d{2})/")


def _existing_manifest(s3, release: str) -> dict | None:
    try:
        obj = s3.get_object(Bucket=config.lake_bucket(), Key=config.manifest_key(release))
    except ClientError as err:
        if err.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return None
        raise
    return json.loads(obj["Body"].read())


def process_object(s3, bucket: str, key: str) -> dict:
    """Ingest one landing object. Returns the manifest (or a skip record)."""
    match = RELEASE_IN_KEY.search(key)
    if not key.startswith(config.LANDING_PREFIX) or not match:
        return {"status": "ignored", "key": key, "why": "key outside price-paid/monthly/release=YYYY-MM/"}
    release = match.group(1)

    obj = s3.get_object(Bucket=bucket, Key=key)
    etag = obj["ETag"].strip('"')
    previous = _existing_manifest(s3, release)
    if previous and previous.get("source_etag") == etag:
        return {"status": "skipped", "key": key, "release": release,
                "why": "same source ETag already processed"}

    body = obj["Body"].read()
    raw = read_raw(body)
    valid, rejected = validate(raw, release)
    curated = to_curated(valid, release)

    lake = config.lake_bucket()
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(curated, preserve_index=False), buf, compression="snappy")
    s3.put_object(Bucket=lake, Key=config.curated_key(release), Body=buf.getvalue())
    s3.put_object(Bucket=lake, Key=config.quarantine_key(release),
                  Body=rejected.to_csv(index=False).encode())

    manifest = {
        "status": "processed",
        "release": release,
        "source": f"s3://{bucket}/{key}",
        "source_etag": etag,
        "source_bytes": len(body),
        "rows_in": len(raw),
        "rows_valid": len(curated),
        "rows_rejected": len(rejected),
        "rejected_by_reason": {k: int(v) for k, v in rejected["reject_reason"].value_counts().items()},
        "record_status": {k: int(v) for k, v in curated["record_status"].value_counts().sort_index().items()},
        "quality_flags": quality_flags(curated),
        "transfer_date_range": [str(curated["date_of_transfer"].min()), str(curated["date_of_transfer"].max())],
        "outputs": {
            "curated": f"s3://{lake}/{config.curated_key(release)}",
            "curated_bytes": buf.getbuffer().nbytes,
            "quarantine": f"s3://{lake}/{config.quarantine_key(release)}",
        },
        "processed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "previous_etag": previous.get("source_etag") if previous else None,
    }
    # Manifest last: it's the commit marker readers check before trusting a release.
    s3.put_object(Bucket=lake, Key=config.manifest_key(release),
                  Body=(json.dumps(manifest, indent=2) + "\n").encode(),
                  ContentType="application/json")
    return manifest


def lambda_handler(event, context=None, s3=None):
    """Entry point. `s3` can be injected for tests; Lambda passes only (event, context)."""
    s3 = s3 or boto3.client("s3")
    results = []
    for record in event.get("Records", []):
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])  # S3 URL-encodes keys in events
        result = process_object(s3, bucket, key)
        print(json.dumps({k: result.get(k) for k in ("status", "release", "rows_valid", "rows_rejected", "why")}))
        results.append(result)
    return {"results": results}
