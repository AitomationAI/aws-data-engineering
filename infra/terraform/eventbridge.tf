# Glue job SUCCEEDED -> Redshift loader Lambda
resource "aws_cloudwatch_event_rule" "glue_succeeded" {
  name        = "${local.name}-glue-succeeded"
  description = "Load curated batches into Redshift when the ETL job succeeds."
  event_pattern = jsonencode({
    source      = ["aws.glue"]
    detail-type = ["Glue Job State Change"]
    detail = {
      jobName = [aws_glue_job.shipment_events.name]
      state   = ["SUCCEEDED"]
    }
  })
}

resource "aws_cloudwatch_event_target" "loader" {
  rule = aws_cloudwatch_event_rule.glue_succeeded.name
  arn  = aws_lambda_function.redshift_loader.arn

  retry_policy {
    maximum_retry_attempts       = 2
    maximum_event_age_in_seconds = 3600
  }
}

resource "aws_lambda_permission" "eventbridge_invoke" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.redshift_loader.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.glue_succeeded.arn
}

# Glue job FAILED / TIMEOUT -> SNS alert
resource "aws_cloudwatch_event_rule" "glue_failed" {
  name = "${local.name}-glue-failed"
  event_pattern = jsonencode({
    source      = ["aws.glue"]
    detail-type = ["Glue Job State Change"]
    detail = {
      jobName = [aws_glue_job.shipment_events.name]
      state   = ["FAILED", "TIMEOUT", "ERROR"]
    }
  })
}

resource "aws_cloudwatch_event_target" "alert" {
  rule = aws_cloudwatch_event_rule.glue_failed.name
  arn  = aws_sns_topic.alerts.arn
}
