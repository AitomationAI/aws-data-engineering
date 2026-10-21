import json

import boto3
import pytest
from moto import mock_aws

from tests.conftest import SRC, load_module

BUCKET = "test-datalake"
JOB = "shipment-events-etl"
GOOD_HEADER = (
    "event_id,shipment_id,carrier_code,event_type,event_ts,location_code,"
    "customer_id,weight_kg,declared_value\n"
)


@pytest.fixture
def aws(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("GLUE_JOB_NAME", JOB)
    with mock_aws():
        handler = load_module("ingest_validator", SRC / "lambdas/ingest_validator/handler.py")
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket=BUCKET)
        boto3.client("glue").create_job(
            Name=JOB, Role="arn:aws:iam::123456789012:role/glue", Command={"Name": "glueetl"}
        )
        yield handler, s3


def s3_event(*keys):
    return {"Records": [{"s3": {"bucket": {"name": BUCKET}, "object": {"key": k}}} for k in keys]}


def test_valid_file_starts_glue_job_with_batch_arguments(aws):
    handler, s3 = aws
    key = "landing/carrier=SWFT/events_2026-09-W1.csv"
    s3.put_object(Bucket=BUCKET, Key=key, Body=GOOD_HEADER + "EVT-1,SHP-1,SWFT,DELIVERED,...\n")

    result = handler.handler(s3_event(key))

    assert result["submitted"] == 1
    run_id = result["results"][0]["job_run_id"]
    args = boto3.client("glue").get_job_run(JobName=JOB, RunId=run_id)["JobRun"]["Arguments"]
    assert args["--input_path"] == f"s3://{BUCKET}/{key}"
    assert len(args["--batch_id"]) == 32


def test_url_encoded_keys_are_decoded(aws):
    handler, s3 = aws
    s3.put_object(Bucket=BUCKET, Key="landing/my file.csv", Body=GOOD_HEADER)
    assert handler.handler(s3_event("landing/my+file.csv"))["submitted"] == 1


@pytest.mark.parametrize(
    ("key", "body", "problem"),
    [
        ("landing/notes.txt", "hello", "unsupported_extension"),
        ("landing/empty.csv", "", "empty_file"),
        ("landing/wrong.csv", "id,shipment,status\n1,2,3\n", "missing_columns:event_id"),
    ],
)
def test_invalid_files_are_quarantined_with_reason(aws, key, body, problem):
    handler, s3 = aws
    s3.put_object(Bucket=BUCKET, Key=key, Body=body)

    result = handler.handler(s3_event(key))["results"][0]

    assert result["status"] == "quarantined"
    assert result["problems"][0].startswith(problem)
    target = key.replace("landing/", "quarantine/")
    keys = {o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET)["Contents"]}
    assert keys == {target, f"{target}.reason.json"}  # moved, not copied
    reason = json.loads(s3.get_object(Bucket=BUCKET, Key=f"{target}.reason.json")["Body"].read())
    assert reason["source_key"] == key


def test_file_size_limit(aws, monkeypatch):
    handler, s3 = aws
    monkeypatch.setenv("MAX_FILE_BYTES", "10")
    s3.put_object(Bucket=BUCKET, Key="landing/big.csv", Body=GOOD_HEADER)
    assert handler.handler(s3_event("landing/big.csv"))["results"][0]["problems"] == ["file_too_large"]
