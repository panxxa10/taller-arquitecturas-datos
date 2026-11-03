output "bucket" { value = aws_s3_bucket.dw.bucket }
output "ec2_public_ip" { value = aws_instance.airflow.public_ip }
output "rds_endpoint" { value = aws_db_instance.mysql.address }
output "rds_secret_arn" { value = aws_db_instance.mysql.master_user_secret[0].secret_arn }
output "docdb_endpoint" { value = aws_docdb_cluster.this.endpoint }
output "docdb_secret_arn" { value = aws_docdb_cluster.this.master_user_secret[0].secret_arn }
output "redshift_endpoint" { value = aws_redshiftserverless_workgroup.this.endpoint[0].address }
output "redshift_secret_arn" { value = aws_redshiftserverless_namespace.this.admin_password_secret_arn }
output "redshift_role_arn" { value = aws_iam_role.redshift_s3.arn }
output "redshift_workgroup" { value = aws_redshiftserverless_workgroup.this.workgroup_name }

output "quicksight_vpc_id" { value = data.aws_vpc.default.id }
output "quicksight_subnet_ids" { value = slice(sort(data.aws_subnets.default.ids), 0, 2) }
output "quicksight_security_group_id" { value = aws_security_group.quicksight.id }
output "quicksight_role_arn" { value = aws_iam_role.quicksight_vpc.arn }
