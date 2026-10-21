"""PySpark unit tests for the Glue job (needs Java; runs in CI and via `make test-spark`)."""

import sys

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

from tests.conftest import SRC, load_module  # noqa: E402

etl = load_module("shipment_events_etl", SRC / "glue_jobs/shipment_events_etl.py")


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("glue-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def raw(spark, rows):
    cols = etl.RAW_COLUMNS + ["source_file"]
    base = {c: None for c in cols}
    return spark.createDataFrame([tuple({**base, **r}[c] for c in cols) for r in rows], cols)


def event(**overrides):
    row = {
        "event_id": "evt-1",
        "shipment_id": "shp-1",
        "carrier_code": " swft ",
        "event_type": "Delivered",
        "event_ts": "2026-09-01T10:00:00",
        "location_code": "nyc1",
        "customer_id": "cust-1",
        "weight_kg": "12.5kg",
        "declared_value": "$1,250.00",
        "source_file": "a.csv",
    }
    return {**row, **overrides}


def test_standardize_cleans_codes_aliases_timestamps_and_numbers(spark):
    rows = etl.standardize(
        raw(spark, [event(), event(event_type="OFD", event_ts="09/01/2026 10:30")])
    ).collect()
    first, second = rows
    assert (first.event_id, first.carrier_code, first.event_type) == ("EVT-1", "SWFT", "DELIVERED")
    assert float(first.weight_kg) == 12.5 and float(first.declared_value) == 1250.0
    assert second.event_type == "OUT_FOR_DELIVERY"
    assert second.event_ts.strftime("%Y-%m-%d %H:%M") == "2026-09-01 10:30"


def test_validate_flags_every_broken_rule(spark):
    df = etl.standardize(
        raw(
            spark,
            [
                event(),
                event(event_id="", weight_kg="-4"),
                event(event_id="evt-3", event_type="TELEPORTED", event_ts="not a date"),
            ],
        )
    )
    valid, rejected = etl.validate(df)
    assert valid.count() == 1
    reasons = sorted(r.reject_reason for r in rejected.collect())
    assert reasons == ["missing_event_id, invalid_weight", "unknown_event_type, invalid_event_ts"]


def test_deduplicate_keeps_one_row_per_event(spark):
    df = etl.standardize(
        raw(spark, [event(source_file="a.csv"), event(source_file="b.csv"), event(event_id="evt-2")])
    )
    result = etl.deduplicate(df).collect()
    assert sorted(r.event_id for r in result) == ["EVT-1", "EVT-2"]
    assert next(r for r in result if r.event_id == "EVT-1").source_file == "b.csv"


def test_transform_produces_curated_schema_with_lineage(spark):
    curated, rejected = etl.transform(raw(spark, [event(), event(weight_kg="x")]), "b" * 32, "2026-10-01")
    assert curated.columns == etl.CURATED_COLUMNS
    row = curated.collect()[0]
    assert row.is_terminal is True
    assert str(row.event_date) == "2026-09-01" and str(row.ingest_date) == "2026-10-01"
    assert rejected.count() == 1


@pytest.fixture
def landed_csv(tmp_path):
    src = tmp_path / "in.csv"
    src.write_text(
        ",".join(etl.RAW_COLUMNS)
        + "\nEVT-1,SHP-1,SWFT,PICKED_UP,2026-09-01T10:00:00,NYC1,C1,2,10\n"
        + "EVT-2,SHP-1,SWFT,DELIVERED,2026-09-02T10:00:00,BOS2,C1,2,10\n"
        + "EVT-2,SHP-1,SWFT,DELIVERED,2026-09-02T10:00:00,BOS2,C1,2,10\n"
        + ",SHP-1,SWFT,DELIVERED,bad,BOS2,C1,2,10\n"
    )
    return src


def test_process_reads_a_landed_file_and_reports_metrics(spark, landed_csv):
    curated, rejected, metrics = etl.process(spark, str(landed_csv), "c" * 32, "2026-10-01")
    assert (metrics["rows_read"], metrics["rows_curated"], metrics["rows_rejected"]) == (4, 2, 1)
    assert metrics["rows_deduplicated"] == 1
    assert curated.first().source_file == "in.csv"


@pytest.mark.skipif(sys.platform == "win32", reason="Spark file writes on Windows need Hadoop winutils; runs in CI")
def test_run_writes_parquet_and_quarantine(spark, landed_csv, tmp_path):
    etl.run(spark, str(landed_csv), str(tmp_path / "out"), "c" * 32, "2026-10-01")
    out = tmp_path / "out/curated/shipment_events/ingest_date=2026-10-01" / f"batch_id={'c' * 32}"
    assert spark.read.parquet(str(out)).count() == 2
    assert spark.read.json(str(tmp_path / "out/quarantine/rejected_events")).count() == 1
