resource "aws_glue_catalog_database" "logistics" {
  name = replace("${local.name}_logistics", "-", "_")
}

resource "aws_glue_job" "shipment_events" {
  name              = "${local.name}-shipment-events-etl"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "5.0"
  worker_type       = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers
  timeout           = 30
  max_retries       = 1

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.datalake.id}/${aws_s3_object.glue_script.key}"
    python_version  = "3"
  }

  execution_property {
    max_concurrent_runs = 5
  }

  default_arguments = {
    "--output_root"                      = "s3://${aws_s3_bucket.datalake.id}"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-job-insights"              = "true"
    "--job-language"                     = "python"
  }
}

# Keeps the Data Catalog (Athena / Redshift Spectrum) in sync with curated data.
resource "aws_glue_crawler" "curated" {
  name          = "${local.name}-curated-crawler"
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.logistics.name
  schedule      = "cron(0 6 * * ? *)"

  s3_target {
    path = "s3://${aws_s3_bucket.datalake.id}/curated/shipment_events/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }
}
