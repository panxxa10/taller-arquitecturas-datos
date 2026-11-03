#!/usr/bin/env bash
# Ejecutar en TU equipo, desde data-warehouse/. Genera .env.aws (sin secretos) desde terraform y copia el proyecto a la EC2.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
TF="terraform -chdir=infra/terraform output -raw"
KEY="${SSH_KEY:-$HOME/.ssh/taller-dw}"
IP="$($TF ec2_public_ip)"

cat > .env.aws <<ENV
AWS_REGION=us-east-1
AWS_DEFAULT_REGION=us-east-1
MYSQL_HOST=$($TF rds_endpoint)
MYSQL_PORT=3306
MYSQL_DATABASE=dw_source
MYSQL_SECRET_ARN=$($TF rds_secret_arn)
DOCDB_HOST=$($TF docdb_endpoint)
DOCDB_SECRET_ARN=$($TF docdb_secret_arn)
MONGO_DATABASE=ecommerce
DOCDB_CA_FILE=/opt/certs/global-bundle.pem
S3_BUCKET=$($TF bucket)
REDSHIFT_HOST=$($TF redshift_endpoint)
REDSHIFT_SECRET_ARN=$($TF redshift_secret_arn)
REDSHIFT_ROLE_ARN=$($TF redshift_role_arn)
REDSHIFT_WORKGROUP=$($TF redshift_workgroup)
REDSHIFT_DATABASE=dw
ENV
chmod 600 .env.aws

rsync -az --delete \
  --exclude '.git' --exclude '.terraform' --exclude '*.tfstate*' --exclude 'tfplan*' --exclude '.venv' --exclude '.venv-dbt' --exclude '.env.airflow' --exclude 'target' --exclude 'logs' \
  -e "ssh -i $KEY -o StrictHostKeyChecking=accept-new" \
  ./ "ec2-user@$IP:~/data-warehouse/"
echo "Proyecto copiado a ec2-user@$IP:~/data-warehouse"
