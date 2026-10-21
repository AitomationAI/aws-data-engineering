data "aws_iam_policy_document" "assume" {
  for_each = toset(["lambda", "glue", "redshift"])
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["${each.key}.amazonaws.com"]
    }
  }
}

# --- Glue job + crawler ------------------------------------------------------
resource "aws_iam_role" "glue" {
  name               = "${local.name}-glue"
  assume_role_policy = data.aws_iam_policy_document.assume["glue"].json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3" {
  role = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = "${aws_s3_bucket.datalake.arn}/*"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = aws_s3_bucket.datalake.arn
      }
    ]
  })
}

# --- Ingest validator Lambda -------------------------------------------------
resource "aws_iam_role" "ingest_validator" {
  name               = "${local.name}-ingest-validator"
  assume_role_policy = data.aws_iam_policy_document.assume["lambda"].json
}

resource "aws_iam_role_policy_attachment" "ingest_validator_logs" {
  role       = aws_iam_role.ingest_validator.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "ingest_validator" {
  role = aws_iam_role.ingest_validator.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = [
          "${aws_s3_bucket.datalake.arn}/landing/*",
          "${aws_s3_bucket.datalake.arn}/quarantine/*",
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["glue:StartJobRun"]
        Resource = aws_glue_job.shipment_events.arn
      },
      {
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = aws_sns_topic.alerts.arn
      }
    ]
  })
}

# --- Redshift loader Lambda --------------------------------------------------
resource "aws_iam_role" "redshift_loader" {
  name               = "${local.name}-redshift-loader"
  assume_role_policy = data.aws_iam_policy_document.assume["lambda"].json
}

resource "aws_iam_role_policy_attachment" "redshift_loader_logs" {
  role       = aws_iam_role.redshift_loader.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "redshift_loader" {
  role = aws_iam_role.redshift_loader.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["glue:GetJobRun"]
        Resource = aws_glue_job.shipment_events.arn
      },
      {
        Effect   = "Allow"
        Action   = ["redshift-data:BatchExecuteStatement", "redshift-data:DescribeStatement"]
        Resource = "*"
      },
      {
        Effect   = "Allow"
        Action   = ["redshift-serverless:GetCredentials"]
        Resource = aws_redshiftserverless_workgroup.this.arn
      },
      {
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = aws_sns_topic.alerts.arn
      }
    ]
  })
}

# --- Role Redshift assumes for COPY from S3 ----------------------------------
resource "aws_iam_role" "redshift_copy" {
  name               = "${local.name}-redshift-copy"
  assume_role_policy = data.aws_iam_policy_document.assume["redshift"].json
}

resource "aws_iam_role_policy" "redshift_copy" {
  role = aws_iam_role.redshift_copy.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = "${aws_s3_bucket.datalake.arn}/curated/*"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = aws_s3_bucket.datalake.arn
      }
    ]
  })
}
