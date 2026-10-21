"""Lambda: load a finished Glue batch into Amazon Redshift Serverless.

Triggered by an EventBridge rule on ``Glue Job State Change`` events. When
the ETL job succeeds it:

1. reads the job run's arguments to find the batch id and curated S3 path
2. renders ``sql/load_batch.sql`` (COPY Parquet -> staging, MERGE -> facts,
   rebuild shipment snapshots, clean staging)
3. submits it through the Redshift Data API as **one** batch. Batches run in
   a single transaction, so a load is all-or-nothing.
"""

from __future__ import annotations

import logging
import os
import re
import time
from functools import cache
from pathlib import Path
from typing import Any

import boto3

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

SQL_FILE = Path(__file__).with_name("sql") / "load_batch.sql"
TERMINAL_STATES = {"FINISHED", "FAILED", "ABORTED"}
_SAFE = {
    "batch_id": re.compile(r"^[0-9a-f]{32}$"),
    "ingest_date": re.compile(r"^\d{4}-\d{2}-\d{2}$"),
}


@cache
def _client(service: str):
    return boto3.client(service)


def render_statements(template: str, values: dict[str, str]) -> list[str]:
    """Substitute ``{{ name }}`` placeholders and split into individual statements."""
    for name, pattern in _SAFE.items():
        if name in values and not pattern.fullmatch(values[name]):
            raise ValueError(f"Refusing to render unsafe value for {name}: {values[name]!r}")
    sql = "\n".join(line for line in template.splitlines() if not line.lstrip().startswith("--"))
    for name, value in values.items():
        sql = sql.replace("{{ " + name + " }}", value)
    if "{{" in sql:
        raise ValueError("Unrendered placeholder left in SQL template")
    return [stmt.strip() for stmt in sql.split(";") if stmt.strip()]


def wait_for(statement_id: str, timeout_s: int, poll_s: float = 2.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_s
    while True:
        status = _client("redshift-data").describe_statement(Id=statement_id)
        if status["Status"] in TERMINAL_STATES or time.monotonic() > deadline:
            return status
        time.sleep(poll_s)


def handler(event: dict[str, Any], _context: Any = None) -> dict[str, Any]:
    detail = event.get("detail", {})
    if detail.get("state") != "SUCCEEDED":
        logger.info("Ignoring Glue state %s for run %s", detail.get("state"), detail.get("jobRunId"))
        return {"status": "skipped", "reason": f"state={detail.get('state')}"}

    run = _client("glue").get_job_run(JobName=detail["jobName"], RunId=detail["jobRunId"])["JobRun"]
    args = run["Arguments"]
    batch_id, ingest_date = args["--batch_id"], args["--ingest_date"]
    curated_bucket = os.environ["CURATED_BUCKET"]

    statements = render_statements(
        SQL_FILE.read_text(encoding="utf-8"),
        {
            "batch_id": batch_id,
            "ingest_date": ingest_date,
            "s3_path": f"s3://{curated_bucket}/curated/shipment_events/"
            f"ingest_date={ingest_date}/batch_id={batch_id}/",
            "iam_role": os.environ["REDSHIFT_COPY_ROLE_ARN"],
        },
    )

    response = _client("redshift-data").batch_execute_statement(
        WorkgroupName=os.environ["REDSHIFT_WORKGROUP"],
        Database=os.environ.get("REDSHIFT_DATABASE", "dev"),
        Sqls=statements,
        StatementName=f"load-shipment-events-{batch_id}",
        WithEvent=True,
    )
    statement_id = response["Id"]
    logger.info("Submitted %d statements for batch %s as %s", len(statements), batch_id, statement_id)

    result = {"status": "submitted", "batch_id": batch_id, "statement_id": statement_id}
    wait_seconds = int(os.environ.get("WAIT_SECONDS", "0"))
    if wait_seconds:
        final = wait_for(statement_id, wait_seconds)
        result["status"] = final["Status"].lower()
        if final["Status"] != "FINISHED":
            raise RuntimeError(f"Redshift load {statement_id} ended {final['Status']}: {final.get('Error')}")
    return result
