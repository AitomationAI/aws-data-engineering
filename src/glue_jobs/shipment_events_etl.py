"""AWS Glue job: raw carrier tracking events (CSV) -> curated Parquet.

Steps
-----
1. read the landed CSV with an explicit all-string schema
2. standardise (trim / upper-case codes, parse mixed timestamp formats, cast numerics)
3. validate - invalid rows get a ``reject_reason`` and go to quarantine as JSON
4. de-duplicate on ``event_id`` (carriers re-send events)
5. enrich (event_date, is_terminal, lineage columns)
6. write Parquet to ``curated/shipment_events/ingest_date=.../batch_id=.../``

Transform functions are pure PySpark, so they are unit-tested locally without
Glue. ``main()`` runs inside Glue; ``--local`` runs the same logic on a laptop.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

RAW_COLUMNS = [
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
RAW_SCHEMA = StructType([StructField(c, StringType(), True) for c in RAW_COLUMNS])

EVENT_TYPES = ["CREATED", "PICKED_UP", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED", "EXCEPTION"]
TERMINAL_EVENTS = ["DELIVERED"]
EVENT_ALIASES = {
    "PICKUP": "PICKED_UP",
    "PICKED UP": "PICKED_UP",
    "TRANSIT": "IN_TRANSIT",
    "IN TRANSIT": "IN_TRANSIT",
    "OUT FOR DELIVERY": "OUT_FOR_DELIVERY",
    "OFD": "OUT_FOR_DELIVERY",
    "DLVD": "DELIVERED",
    "EXC": "EXCEPTION",
}
TIMESTAMP_FORMATS = ["yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd HH:mm:ss", "MM/dd/yyyy HH:mm"]

CURATED_COLUMNS = [
    "event_id",
    "shipment_id",
    "carrier_code",
    "event_type",
    "event_ts",
    "event_date",
    "location_code",
    "customer_id",
    "weight_kg",
    "declared_value",
    "is_terminal",
    "source_file",
    "batch_id",
    "ingest_date",
]


def _clean(col: str) -> F.Column:
    trimmed = F.trim(F.regexp_replace(F.col(col), r"\s+", " "))
    return F.when(trimmed == "", None).otherwise(trimmed)


def standardize(df: DataFrame) -> DataFrame:
    event_type = F.upper(_clean("event_type"))
    alias_map = F.create_map(*[F.lit(x) for pair in EVENT_ALIASES.items() for x in pair])
    parsed_ts = F.coalesce(*[F.try_to_timestamp(_clean("event_ts"), F.lit(f)) for f in TIMESTAMP_FORMATS])
    return df.select(
        F.upper(_clean("event_id")).alias("event_id"),
        F.upper(_clean("shipment_id")).alias("shipment_id"),
        F.upper(_clean("carrier_code")).alias("carrier_code"),
        F.coalesce(alias_map[event_type], event_type).alias("event_type"),
        parsed_ts.alias("event_ts"),
        F.upper(_clean("location_code")).alias("location_code"),
        F.upper(_clean("customer_id")).alias("customer_id"),
        F.regexp_replace(_clean("weight_kg"), "[^0-9.-]", "").cast("decimal(10,2)").alias("weight_kg"),
        F.regexp_replace(_clean("declared_value"), "[$,]", "").cast("decimal(12,2)").alias("declared_value"),
        F.col("source_file"),
    )


def validate(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split into (valid, rejected); rejected rows carry every rule they broke."""
    checks = [
        (F.col("event_id").isNull(), "missing_event_id"),
        (F.col("shipment_id").isNull(), "missing_shipment_id"),
        (F.col("carrier_code").isNull(), "missing_carrier"),
        (~F.col("event_type").isin(EVENT_TYPES) | F.col("event_type").isNull(), "unknown_event_type"),
        (F.col("event_ts").isNull(), "invalid_event_ts"),
        (F.col("weight_kg").isNull() | (F.col("weight_kg") <= 0), "invalid_weight"),
        (F.col("declared_value") < 0, "negative_declared_value"),
    ]
    reasons = F.concat_ws(", ", *[F.when(cond, F.lit(name)) for cond, name in checks])
    flagged = df.withColumn("reject_reason", F.when(reasons != "", reasons))
    valid = flagged.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    rejected = flagged.filter(F.col("reject_reason").isNotNull())
    return valid, rejected


def deduplicate(df: DataFrame) -> DataFrame:
    """Keep one row per event_id - the most complete, latest-file version."""
    window = Window.partitionBy("event_id").orderBy(
        F.col("declared_value").isNull().asc(), F.col("source_file").desc()
    )
    return df.withColumn("_rn", F.row_number().over(window)).filter("_rn = 1").drop("_rn")


def enrich(df: DataFrame, batch_id: str, ingest_date: str) -> DataFrame:
    return (
        df.withColumn("event_date", F.to_date("event_ts"))
        .withColumn("is_terminal", F.col("event_type").isin(TERMINAL_EVENTS))
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("ingest_date", F.lit(ingest_date).cast("date"))
        .select(*CURATED_COLUMNS)
    )


def transform(raw: DataFrame, batch_id: str, ingest_date: str) -> tuple[DataFrame, DataFrame]:
    valid, rejected = validate(standardize(raw))
    return enrich(deduplicate(valid), batch_id, ingest_date), rejected


def read_raw(spark: SparkSession, input_path: str) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(RAW_SCHEMA)
        .csv(input_path)
        .withColumn("source_file", F.element_at(F.split(F.input_file_name(), "/"), -1))
    )


def process(
    spark: SparkSession, input_path: str, batch_id: str, ingest_date: str
) -> tuple[DataFrame, DataFrame, dict[str, Any]]:
    """Read + transform + compute run metrics. All logic, no writes."""
    raw = read_raw(spark, input_path).cache()
    curated, rejected = transform(raw, batch_id, ingest_date)
    curated, rejected = curated.cache(), rejected.cache()
    metrics = {
        "batch_id": batch_id,
        "rows_read": raw.count(),
        "rows_curated": curated.count(),
        "rows_rejected": rejected.count(),
        "reject_reasons": {
            r["reject_reason"]: r["count"] for r in rejected.groupBy("reject_reason").count().collect()
        },
    }
    metrics["rows_deduplicated"] = metrics["rows_read"] - metrics["rows_rejected"] - metrics["rows_curated"]
    return curated, rejected, metrics


def write_outputs(curated: DataFrame, rejected: DataFrame, output_root: str, batch_id: str, ingest_date: str) -> None:
    partition = f"ingest_date={ingest_date}/batch_id={batch_id}"
    curated.coalesce(4).write.mode("overwrite").parquet(f"{output_root}/curated/shipment_events/{partition}")
    rejected.write.mode("overwrite").json(f"{output_root}/quarantine/rejected_events/{partition}")


def run(
    spark: SparkSession, input_path: str, output_root: str, batch_id: str, ingest_date: str
) -> dict[str, Any]:
    curated, rejected, metrics = process(spark, input_path, batch_id, ingest_date)
    write_outputs(curated, rejected, output_root, batch_id, ingest_date)
    return metrics


def main(argv: list[str]) -> None:
    if "--local" in argv:
        parser = argparse.ArgumentParser()
        parser.add_argument("--local", action="store_true")
        parser.add_argument("--input_path", required=True)
        parser.add_argument("--output_root", default="output")
        parser.add_argument("--batch_id", default="0" * 32)
        parser.add_argument("--ingest_date", default="2026-10-01")
        args = parser.parse_args(argv[1:])
        spark = SparkSession.builder.appName("shipment-events-etl-local").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        print(
            json.dumps(
                run(spark, args.input_path, args.output_root, args.batch_id, args.ingest_date), indent=2
            )
        )
        return

    from awsglue.context import GlueContext  # only available inside AWS Glue
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions

    args = getResolvedOptions(argv, ["JOB_NAME", "input_path", "output_root", "batch_id", "ingest_date"])
    glue_context = GlueContext(SparkSession.builder.getOrCreate().sparkContext)
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)
    metrics = run(
        glue_context.spark_session,
        args["input_path"],
        args["output_root"],
        args["batch_id"],
        args["ingest_date"],
    )
    print(json.dumps(metrics))  # lands in CloudWatch Logs
    job.commit()


if __name__ == "__main__":
    main(sys.argv)
