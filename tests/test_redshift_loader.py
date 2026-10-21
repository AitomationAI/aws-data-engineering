import boto3
import pytest
from moto import mock_aws

from tests.conftest import SRC, load_module

JOB = "shipment-events-etl"
BATCH = "a" * 32


@pytest.fixture
def aws(monkeypatch):
    for name, value in {
        "AWS_DEFAULT_REGION": "us-east-1",
        "CURATED_BUCKET": "test-datalake",
        "REDSHIFT_WORKGROUP": "logistics-wg",
        "REDSHIFT_COPY_ROLE_ARN": "arn:aws:iam::123456789012:role/redshift-copy",
    }.items():
        monkeypatch.setenv(name, value)
    with mock_aws():
        handler = load_module("redshift_loader", SRC / "lambdas/redshift_loader/handler.py")
        glue = boto3.client("glue")
        glue.create_job(Name=JOB, Role="arn:aws:iam::123456789012:role/g", Command={"Name": "glueetl"})
        run_id = glue.start_job_run(
            JobName=JOB, Arguments={"--batch_id": BATCH, "--ingest_date": "2026-10-01"}
        )["JobRunId"]
        yield handler, run_id


def glue_event(run_id, state="SUCCEEDED"):
    return {"detail": {"jobName": JOB, "jobRunId": run_id, "state": state}}


def test_render_statements_fills_placeholders_and_drops_comments():
    handler = load_module("redshift_loader", SRC / "lambdas/redshift_loader/handler.py")
    sql = handler.SQL_FILE.read_text()
    stmts = handler.render_statements(
        sql, {"batch_id": BATCH, "ingest_date": "2026-10-01", "s3_path": "s3://b/p/", "iam_role": "arn:r"}
    )
    assert len(stmts) == 8
    assert stmts[1].startswith("COPY logistics.stg_shipment_events")
    assert "FROM 's3://b/p/'" in stmts[1] and "IAM_ROLE 'arn:r'" in stmts[1]
    assert stmts[3].startswith("MERGE INTO logistics.fact_shipment_events")
    assert not any("{{" in s or "--" in s for s in stmts)


@pytest.mark.parametrize("bad", ["x'; DROP TABLE t; --", "ABC", "1" * 31])
def test_render_rejects_unsafe_batch_ids(bad):
    handler = load_module("redshift_loader", SRC / "lambdas/redshift_loader/handler.py")
    with pytest.raises(ValueError, match="unsafe"):
        handler.render_statements("SELECT '{{ batch_id }}'", {"batch_id": bad})


class FakeRedshiftData:
    """moto does not implement batch_execute_statement, so record calls instead."""

    def __init__(self, final_status="FINISHED"):
        self.calls = []
        self.final_status = final_status

    def batch_execute_statement(self, **kwargs):
        self.calls.append(kwargs)
        return {"Id": "stmt-123"}

    def describe_statement(self, Id):
        return {"Id": Id, "Status": self.final_status, "Error": "boom"}


@pytest.fixture
def fake_rsd(aws, monkeypatch):
    handler, _ = aws
    fake = FakeRedshiftData()
    real = handler._client
    monkeypatch.setattr(handler, "_client", lambda s: fake if s == "redshift-data" else real(s))
    return fake


def test_successful_glue_run_submits_one_transactional_batch(aws, fake_rsd):
    handler, run_id = aws

    result = handler.handler(glue_event(run_id))

    assert result == {"status": "submitted", "batch_id": BATCH, "statement_id": "stmt-123"}
    (call,) = fake_rsd.calls
    assert call["WorkgroupName"] == "logistics-wg"
    assert call["StatementName"] == f"load-shipment-events-{BATCH}"
    copy = next(s for s in call["Sqls"] if s.startswith("COPY"))
    assert f"s3://test-datalake/curated/shipment_events/ingest_date=2026-10-01/batch_id={BATCH}/" in copy


def test_waits_for_completion_when_configured(aws, fake_rsd, monkeypatch):
    handler, run_id = aws
    monkeypatch.setenv("WAIT_SECONDS", "5")
    assert handler.handler(glue_event(run_id))["status"] == "finished"


def test_failed_redshift_load_raises_so_lambda_retries(aws, fake_rsd, monkeypatch):
    handler, run_id = aws
    monkeypatch.setenv("WAIT_SECONDS", "5")
    fake_rsd.final_status = "FAILED"
    with pytest.raises(RuntimeError, match="FAILED: boom"):
        handler.handler(glue_event(run_id))


def test_non_success_states_are_ignored(aws):
    handler, run_id = aws
    assert handler.handler(glue_event(run_id, state="FAILED"))["status"] == "skipped"
