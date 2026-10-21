data "archive_file" "ingest_validator" {
  type        = "zip"
  source_dir  = "${local.src_dir}/lambdas/ingest_validator"
  output_path = "${path.module}/.build/ingest_validator.zip"
}

data "archive_file" "redshift_loader" {
  type        = "zip"
  source_dir  = "${local.src_dir}/lambdas/redshift_loader"
  output_path = "${path.module}/.build/redshift_loader.zip"
}

resource "aws_lambda_function" "ingest_validator" {
  function_name    = "${local.name}-ingest-validator"
  role             = aws_iam_role.ingest_validator.arn
  runtime          = "python3.12"
  handler          = "handler.handler"
  filename         = data.archive_file.ingest_validator.output_path
  source_code_hash = data.archive_file.ingest_validator.output_base64sha256
  timeout          = 60
  memory_size      = 256

  environment {
    variables = {
      GLUE_JOB_NAME  = aws_glue_job.shipment_events.name
      MAX_FILE_BYTES = tostring(500 * 1024 * 1024)
    }
  }

  dead_letter_config {
    target_arn = aws_sns_topic.alerts.arn
  }
}

resource "aws_lambda_function" "redshift_loader" {
  function_name    = "${local.name}-redshift-loader"
  role             = aws_iam_role.redshift_loader.arn
  runtime          = "python3.12"
  handler          = "handler.handler"
  filename         = data.archive_file.redshift_loader.output_path
  source_code_hash = data.archive_file.redshift_loader.output_base64sha256
  timeout          = 300
  memory_size      = 256

  environment {
    variables = {
      CURATED_BUCKET         = aws_s3_bucket.datalake.id
      REDSHIFT_WORKGROUP     = aws_redshiftserverless_workgroup.this.workgroup_name
      REDSHIFT_DATABASE      = var.redshift_database
      REDSHIFT_COPY_ROLE_ARN = aws_iam_role.redshift_copy.arn
      WAIT_SECONDS           = "240"
    }
  }

  dead_letter_config {
    target_arn = aws_sns_topic.alerts.arn
  }
}

resource "aws_cloudwatch_log_group" "ingest_validator" {
  name              = "/aws/lambda/${aws_lambda_function.ingest_validator.function_name}"
  retention_in_days = 30
}

resource "aws_cloudwatch_log_group" "redshift_loader" {
  name              = "/aws/lambda/${aws_lambda_function.redshift_loader.function_name}"
  retention_in_days = 30
}

# S3 landing/ -> ingest validator
resource "aws_lambda_permission" "s3_invoke" {
  statement_id  = "AllowS3Invoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingest_validator.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.datalake.arn
}

resource "aws_s3_bucket_notification" "landing" {
  bucket = aws_s3_bucket.datalake.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.ingest_validator.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "landing/"
    filter_suffix       = ".csv"
  }

  depends_on = [aws_lambda_permission.s3_invoke]
}
