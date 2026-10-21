output "datalake_bucket" {
  description = "Upload carrier files to s3://<bucket>/landing/ to trigger the pipeline."
  value       = aws_s3_bucket.datalake.id
}

output "glue_job_name" {
  value = aws_glue_job.shipment_events.name
}

output "redshift_workgroup" {
  value = aws_redshiftserverless_workgroup.this.workgroup_name
}

output "redshift_endpoint" {
  value = aws_redshiftserverless_workgroup.this.endpoint
}

output "alerts_topic_arn" {
  value = aws_sns_topic.alerts.arn
}
