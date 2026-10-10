"""Bucket setup with sensible defaults, plus helpers for running locally.

`ensure_buckets` is idempotent and uses only standard S3 API calls, so it
works the same against AWS or the moto emulator.
"""
import os
from contextlib import contextmanager
from urllib.parse import quote_plus

from botocore.exceptions import ClientError

from lake import config

QUARANTINE_RETENTION_DAYS = 90
NONCURRENT_VERSION_RETENTION_DAYS = 30

# Keeps versioning from growing storage forever: overwritten versions go after 30 days.
EXPIRE_OLD_VERSIONS = {
    "ID": "expire-old-versions",
    "Filter": {},
    "Status": "Enabled",
    "NoncurrentVersionExpiration": {"NoncurrentDays": NONCURRENT_VERSION_RETENTION_DAYS},
}


def ensure_buckets(s3, region: str) -> list[str]:
    """Create the landing and lake buckets if needed and apply the defaults:
    private (public access blocked), encrypted at rest (SSE-S3), versioned
    (recover from bad overwrites, old versions kept 30 days), and quarantine
    files expiring after 90 days. infra/template.yaml mirrors these settings.
    """
    created = []
    for bucket in (config.landing_bucket(), config.lake_bucket()):
        try:
            s3.head_bucket(Bucket=bucket)
        except ClientError:
            kwargs = {} if region == "us-east-1" else {
                "CreateBucketConfiguration": {"LocationConstraint": region}}
            s3.create_bucket(Bucket=bucket, **kwargs)
            created.append(bucket)
        s3.put_public_access_block(Bucket=bucket, PublicAccessBlockConfiguration={
            "BlockPublicAcls": True, "IgnorePublicAcls": True,
            "BlockPublicPolicy": True, "RestrictPublicBuckets": True})
        s3.put_bucket_encryption(Bucket=bucket, ServerSideEncryptionConfiguration={
            "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]})
        s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
    s3.put_bucket_lifecycle_configuration(
        Bucket=config.landing_bucket(),
        LifecycleConfiguration={"Rules": [EXPIRE_OLD_VERSIONS]},
    )
    s3.put_bucket_lifecycle_configuration(
        Bucket=config.lake_bucket(),
        LifecycleConfiguration={"Rules": [{
            "ID": "expire-quarantine",
            "Filter": {"Prefix": config.QUARANTINE_PREFIX},
            "Status": "Enabled",
            "Expiration": {"Days": QUARANTINE_RETENTION_DAYS},
        }, EXPIRE_OLD_VERSIONS]},
    )
    return created


def s3_put_event(bucket: str, key: str, etag: str, size: int, region: str) -> dict:
    """Build the event S3 sends to Lambda on ObjectCreated:Put (trimmed to the
    fields the handler reads, plus a few for realism). Keys are URL-encoded,
    as in real events."""
    return {"Records": [{
        "eventVersion": "2.1",
        "eventSource": "aws:s3",
        "awsRegion": region,
        "eventName": "ObjectCreated:Put",
        "s3": {
            "s3SchemaVersion": "1.0",
            "bucket": {"name": bucket, "arn": f"arn:aws:s3:::{bucket}"},
            "object": {"key": quote_plus(key, safe="/"), "size": size, "eTag": etag},
        },
    }]}


@contextmanager
def local_aws(port: int = 5055, region: str = "eu-west-2"):
    """Run a moto S3 server on localhost and point boto3 (and DuckDB) at it.

    Dummy credentials only: nothing leaves the machine.
    """
    from moto.server import ThreadedMotoServer

    server = ThreadedMotoServer(ip_address="127.0.0.1", port=port, verbose=False)
    server.start()
    saved = {k: os.environ.get(k) for k in (
        "AWS_ENDPOINT_URL", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_DEFAULT_REGION")}
    os.environ.update({
        "AWS_ENDPOINT_URL": f"http://127.0.0.1:{port}",
        "AWS_ACCESS_KEY_ID": "local-test",
        "AWS_SECRET_ACCESS_KEY": "local-test",
        "AWS_DEFAULT_REGION": region,
    })
    try:
        yield f"127.0.0.1:{port}"
    finally:
        server.stop()
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
