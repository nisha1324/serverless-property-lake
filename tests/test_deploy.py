"""Offline checks on the deployment template (infra/template.yaml).

- the template is valid CloudFormation/SAM (cfn-lint);
- it agrees with the code: bucket env vars, lake prefixes, event filter,
  retention settings;
- the function role is least-privilege: the handler runs as that role against
  moto *with IAM enforcement switched on* and succeeds, while writes outside
  its prefixes, deletes and landing-bucket writes are denied;
- the uploader policy can only drop files into the monthly prefix;
- the Lambda bundle holds only the handler's modules.
"""
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import boto3
import pytest
import yaml
from botocore.exceptions import ClientError
from moto import mock_aws
from moto.core.authorization import enable_iam_authentication

from lake import config
from lake import infra as lake_infra
from lake.handler import lambda_handler
from lake.infra import ensure_buckets, s3_put_event
from test_lake import SAMPLE, to_csv

ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / "infra" / "template.yaml"
REGION = "eu-west-2"
RELEASE = "2026-09"
STACK, ACCOUNT = "ppd-lake", "123456789012"  # moto's default account id
LANDING, LAKE = f"{STACK}-landing-{ACCOUNT}", f"{STACK}-lake-{ACCOUNT}"


class CfnLoader(yaml.SafeLoader):
    """Reads CloudFormation short-form tags (!Sub, !Ref, ...) as plain dicts."""


def _tag(loader, suffix, node):
    name = "Ref" if suffix == "Ref" else f"Fn::{suffix}"
    if isinstance(node, yaml.ScalarNode):
        return {name: loader.construct_scalar(node)}
    if isinstance(node, yaml.SequenceNode):
        return {name: loader.construct_sequence(node, deep=True)}
    return {name: loader.construct_mapping(node, deep=True)}


CfnLoader.add_multi_constructor("!", _tag)


@pytest.fixture(scope="module")
def template() -> dict:
    return yaml.load(TEMPLATE.read_text(), Loader=CfnLoader)


def resolve(value):
    """Resolve !Sub / !Ref for a stack called ppd-lake in moto's account."""
    names = {"AWS::StackName": STACK, "AWS::AccountId": ACCOUNT, "AWS::Partition": "aws",
             "LandingBucket": LANDING, "LakeBucket": LAKE}
    if isinstance(value, dict) and "Fn::Sub" in value:
        return re.sub(r"\$\{([^}]+)\}", lambda m: names[m.group(1)], value["Fn::Sub"])
    if isinstance(value, dict) and "Ref" in value:
        return names[value["Ref"]]
    if isinstance(value, list):
        return [resolve(v) for v in value]
    if isinstance(value, dict):
        return {k: resolve(v) for k, v in value.items()}
    return value


def function_policy(template) -> dict:
    return resolve(template["Resources"]["IngestFunction"]["Properties"]["Policies"][0])


def test_template_passes_cfn_lint():
    cfnlint = pytest.importorskip("cfnlint.api")
    matches = cfnlint.lint_file(TEMPLATE)
    assert [str(m) for m in matches] == []


def test_template_agrees_with_the_code(template):
    res = template["Resources"]
    fn = res["IngestFunction"]["Properties"]
    assert fn["Handler"] == "lake.handler.lambda_handler"
    assert resolve(fn["Environment"]["Variables"]) == {"LANDING_BUCKET": LANDING, "LAKE_BUCKET": LAKE}
    rules = fn["Events"]["MonthlyFile"]["Properties"]["Filter"]["S3Key"]["Rules"]
    assert {r["Name"]: r["Value"] for r in rules} == {"prefix": config.LANDING_PREFIX, "suffix": ".csv"}

    statements = {s["Sid"]: s for s in function_policy(template)["Statement"]}
    assert set(statements["WriteLakeOutputs"]["Resource"]) == {
        f"arn:aws:s3:::{LAKE}/{p}*" for p in
        (config.CURATED_PREFIX, config.QUARANTINE_PREFIX, config.MANIFEST_PREFIX)}
    actions = {a for s in statements.values() for a in [s["Action"]]}
    assert actions == {"s3:GetObject", "s3:PutObject", "s3:ListBucket"}  # no Delete, no wildcards

    lake_rules = {r["Id"]: r for r in res["LakeBucket"]["Properties"]["LifecycleConfiguration"]["Rules"]}
    assert lake_rules["expire-quarantine"]["Prefix"] == config.QUARANTINE_PREFIX
    params = template["Parameters"]
    assert params["QuarantineRetentionDays"]["Default"] == lake_infra.QUARANTINE_RETENTION_DAYS
    assert params["NoncurrentVersionRetentionDays"]["Default"] == lake_infra.NONCURRENT_VERSION_RETENTION_DAYS


@pytest.fixture
def aws(monkeypatch):
    for k, v in {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test",
                 "AWS_DEFAULT_REGION": REGION, "LANDING_BUCKET": LANDING, "LAKE_BUCKET": LAKE}.items():
        monkeypatch.setenv(k, v)
    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    with mock_aws():
        yield


def client_as(name: str, policy: dict, trust_service: str = "lambda.amazonaws.com"):
    """Create a role with only `policy`, assume it, return an S3 client using it."""
    iam = boto3.client("iam", region_name=REGION)
    trust = {"Version": "2012-10-17", "Statement": [{"Effect": "Allow", "Action": "sts:AssumeRole",
             "Principal": {"Service": trust_service}}]}
    role = iam.create_role(RoleName=name, AssumeRolePolicyDocument=json.dumps(trust))["Role"]
    iam.put_role_policy(RoleName=name, PolicyName=name, PolicyDocument=json.dumps(policy))
    creds = boto3.client("sts", region_name=REGION).assume_role(
        RoleArn=role["Arn"], RoleSessionName="test")["Credentials"]
    return boto3.client("s3", region_name=REGION, aws_access_key_id=creds["AccessKeyId"],
                        aws_secret_access_key=creds["SecretAccessKey"],
                        aws_session_token=creds["SessionToken"])


def denied(call) -> bool:
    try:
        call()
    except ClientError as err:
        return err.response["Error"]["Code"] == "AccessDenied"
    return False


def test_handler_runs_with_only_the_function_role(aws, template):
    admin = boto3.client("s3", region_name=REGION)
    ensure_buckets(admin, REGION)
    key = config.landing_key(RELEASE)
    admin.put_object(Bucket=LANDING, Key=key, Body=to_csv(SAMPLE))
    admin.put_object(Bucket=LANDING, Key="other/notes.csv", Body=b"x")
    etag = admin.head_object(Bucket=LANDING, Key=key)["ETag"].strip('"')
    event = s3_put_event(LANDING, key, etag, 1, REGION)
    role_s3 = client_as("ingest-fn", function_policy(template))

    with enable_iam_authentication():
        assert lambda_handler(event, s3=role_s3)["results"][0]["status"] == "processed"
        assert lambda_handler(event, s3=role_s3)["results"][0]["status"] == "skipped"
        put = role_s3.put_object
        assert denied(lambda: put(Bucket=LAKE, Key="elsewhere/x.parquet", Body=b"x"))
        assert denied(lambda: put(Bucket=LANDING, Key=key, Body=b"x"))
        assert denied(lambda: role_s3.delete_object(Bucket=LAKE, Key=config.curated_key(RELEASE)))
        assert denied(lambda: role_s3.get_object(Bucket=LANDING, Key="other/notes.csv"))
        assert denied(lambda: role_s3.put_bucket_policy(Bucket=LAKE, Policy="{}"))


def test_uploader_can_only_drop_monthly_files(aws, template):
    ensure_buckets(boto3.client("s3", region_name=REGION), REGION)
    policy = resolve(template["Resources"]["LandingUploaderPolicy"]["Properties"]["PolicyDocument"])
    up = client_as("uploader", policy, trust_service="ec2.amazonaws.com")
    with enable_iam_authentication():
        up.put_object(Bucket=LANDING, Key=config.landing_key(RELEASE), Body=b"csv")
        assert denied(lambda: up.put_object(Bucket=LANDING, Key="anything.csv", Body=b"x"))
        assert denied(lambda: up.put_object(Bucket=LAKE, Key=config.curated_key(RELEASE), Body=b"x"))
        assert denied(lambda: up.get_object(Bucket=LANDING, Key=config.landing_key(RELEASE)))


def test_old_versions_expire_on_both_buckets(aws):
    s3 = boto3.client("s3", region_name=REGION)
    ensure_buckets(s3, REGION)
    for bucket in (LANDING, LAKE):
        rules = s3.get_bucket_lifecycle_configuration(Bucket=bucket)["Rules"]
        days = [r["NoncurrentVersionExpiration"]["NoncurrentDays"]
                for r in rules if "NoncurrentVersionExpiration" in r]
        assert days == [lake_infra.NONCURRENT_VERSION_RETENTION_DAYS]


def test_lambda_bundle_is_only_the_handler_modules(tmp_path):
    out = tmp_path / "function"
    subprocess.run([sys.executable, str(ROOT / "infra" / "package.py"), "--out", str(out)], check=True)
    assert sorted(p.name for p in (out / "lake").iterdir()) == [
        "__init__.py", "config.py", "handler.py", "schema.py"]
    # Import the bundle in a clean interpreter (no repo on the path): it must
    # load from the bundle and pull in neither DuckDB nor moto.
    probe = ("import sys, lake.handler as h; "
             "print(h.__file__); print('duckdb' in sys.modules or 'moto' in sys.modules)")
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    lines = subprocess.run([sys.executable, "-I", "-c", f"import sys; sys.path.insert(0, {str(out)!r}); {probe}"],
                           capture_output=True, text=True, check=True, env=env).stdout.split()
    assert lines[0].startswith(str(out)) and lines[1] == "False"
