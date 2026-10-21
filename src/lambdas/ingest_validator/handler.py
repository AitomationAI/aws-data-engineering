"""Lambda: validate files landing in S3 and hand good ones to AWS Glue.

Triggered by ``s3:ObjectCreated:*`` on ``landing/``. For every new object it
runs cheap pre-flight checks (extension, size, CSV header) without
downloading the whole file:

* **valid**   -> starts the Glue ETL job with the file path and a new batch id
* **invalid** -> moves the file to ``quarantine/`` together with a
                 ``.reason.json`` sidecar explaining why
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from functools import cache
from typing import Any
from urllib.parse import unquote_plus

import boto3

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

EXPECTED_COLUMNS = [
    "event_id",
    "shipment_id",
    "carrier_code",
    "event_type",
    "event_ts",
    "location_code",
    "customer_id",
    "weight_kg",
    "declared_value",
]
ALLOWED_EXTENSIONS = (".csv",)
HEADER_PROBE_BYTES = 4096


@cache
def _client(service: str):
    return boto3.client(service)


def _env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        raise RuntimeError(f"Missing required environment variable {name}")
    return value


def inspect_object(bucket: str, key: str) -> list[str]:
    """Return a list of problems with the object; empty means it is loadable."""
    problems: list[str] = []
    if not key.lower().endswith(ALLOWED_EXTENSIONS):
        problems.append("unsupported_extension")
        return problems

    size = _client("s3").head_object(Bucket=bucket, Key=key)["ContentLength"]
    max_bytes = int(_env("MAX_FILE_BYTES", str(500 * 1024 * 1024)))
    if size == 0:
        return ["empty_file"]
    if size > max_bytes:
        problems.append("file_too_large")

    probe = _client("s3").get_object(Bucket=bucket, Key=key, Range=f"bytes=0-{HEADER_PROBE_BYTES - 1}")
    first_line = probe["Body"].read().decode("utf-8-sig", errors="replace").splitlines()[0]
    header = [col.strip().lower() for col in next(csv.reader(io.StringIO(first_line)))]
    missing = [col for col in EXPECTED_COLUMNS if col not in header]
    if missing:
        problems.append(f"missing_columns:{','.join(missing)}")
    return problems


def quarantine(bucket: str, key: str, problems: list[str]) -> str:
    target = "quarantine/" + key.removeprefix("landing/")
    s3 = _client("s3")
    s3.copy_object(Bucket=bucket, Key=target, CopySource={"Bucket": bucket, "Key": key})
    s3.put_object(
        Bucket=bucket,
        Key=f"{target}.reason.json",
        Body=json.dumps(
            {
                "source_key": key,
                "problems": problems,
                "quarantined_at": datetime.now(timezone.utc).isoformat(),
            }
        ).encode(),
        ContentType="application/json",
    )
    s3.delete_object(Bucket=bucket, Key=key)
    return target


def start_glue_job(bucket: str, key: str) -> dict[str, str]:
    batch_id = uuid.uuid4().hex
    ingest_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    response = _client("glue").start_job_run(
        JobName=_env("GLUE_JOB_NAME"),
        Arguments={
            "--input_path": f"s3://{bucket}/{key}",
            "--batch_id": batch_id,
            "--ingest_date": ingest_date,
        },
    )
    return {"batch_id": batch_id, "ingest_date": ingest_date, "job_run_id": response["JobRunId"]}


def process_record(record: dict[str, Any]) -> dict[str, Any]:
    bucket = record["s3"]["bucket"]["name"]
    key = unquote_plus(record["s3"]["object"]["key"])

    problems = inspect_object(bucket, key)
    if problems:
        target = quarantine(bucket, key, problems)
        logger.warning("Quarantined s3://%s/%s -> %s: %s", bucket, key, target, problems)
        return {"key": key, "status": "quarantined", "problems": problems, "target": target}

    run = start_glue_job(bucket, key)
    logger.info("Submitted s3://%s/%s to Glue: %s", bucket, key, run)
    return {"key": key, "status": "submitted", **run}


def handler(event: dict[str, Any], _context: Any = None) -> dict[str, Any]:
    results = [process_record(record) for record in event.get("Records", [])]
    return {
        "processed": len(results),
        "submitted": sum(r["status"] == "submitted" for r in results),
        "quarantined": sum(r["status"] == "quarantined" for r in results),
        "results": results,
    }
