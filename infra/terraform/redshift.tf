resource "aws_redshiftserverless_namespace" "this" {
  namespace_name        = local.name
  db_name               = var.redshift_database
  admin_username        = "admin"
  manage_admin_password = true # generated and stored in Secrets Manager
  iam_roles             = [aws_iam_role.redshift_copy.arn]
  default_iam_role_arn  = aws_iam_role.redshift_copy.arn
}

resource "aws_redshiftserverless_workgroup" "this" {
  namespace_name      = aws_redshiftserverless_namespace.this.namespace_name
  workgroup_name      = local.name
  base_capacity       = var.redshift_base_capacity
  publicly_accessible = false
}
