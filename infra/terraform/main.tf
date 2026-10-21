data "aws_caller_identity" "current" {}

locals {
  name        = "${var.project}-${var.environment}"
  bucket_name = "${local.name}-datalake-${data.aws_caller_identity.current.account_id}"
  src_dir     = "${path.module}/../../src"
}

# ---------------------------------------------------------------------------
# S3 data lake: landing/ -> (Glue) -> curated/   bad files -> quarantine/
# ---------------------------------------------------------------------------
resource "aws_s3_bucket" "datalake" {
  bucket = local.bucket_name
}

resource "aws_s3_bucket_versioning" "datalake" {
  bucket = aws_s3_bucket.datalake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "datalake" {
  bucket = aws_s3_bucket.datalake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "datalake" {
  bucket                  = aws_s3_bucket.datalake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "datalake" {
  bucket = aws_s3_bucket.datalake.id

  rule {
    id     = "expire-landing"
    status = "Enabled"
    filter {
      prefix = "landing/"
    }
    expiration {
      days = 30
    }
  }

  rule {
    id     = "archive-curated"
    status = "Enabled"
    filter {
      prefix = "curated/"
    }
    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }
  }
}

resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.datalake.id
  key    = "scripts/shipment_events_etl.py"
  source = "${local.src_dir}/glue_jobs/shipment_events_etl.py"
  etag   = filemd5("${local.src_dir}/glue_jobs/shipment_events_etl.py")
}

# ---------------------------------------------------------------------------
# Alerts
# ---------------------------------------------------------------------------
resource "aws_sns_topic" "alerts" {
  name = "${local.name}-alerts"
}

resource "aws_sns_topic_subscription" "email" {
  count     = var.alert_email == "" ? 0 : 1
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_sns_topic_policy" "alerts" {
  arn = aws_sns_topic.alerts.arn
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "events.amazonaws.com" }
      Action    = "sns:Publish"
      Resource  = aws_sns_topic.alerts.arn
    }]
  })
}
