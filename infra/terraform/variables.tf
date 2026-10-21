variable "project" {
  description = "Prefix for every resource name."
  type        = string
  default     = "shipment-pipeline"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)."
  type        = string
  default     = "dev"
}

variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "glue_worker_type" {
  type    = string
  default = "G.1X"
}

variable "glue_number_of_workers" {
  type    = number
  default = 2
}

variable "redshift_base_capacity" {
  description = "Redshift Serverless base RPUs (8 is the minimum)."
  type        = number
  default     = 8
}

variable "redshift_database" {
  type    = string
  default = "logistics"
}

variable "alert_email" {
  description = "Optional e-mail subscribed to pipeline failure alerts."
  type        = string
  default     = ""
}
