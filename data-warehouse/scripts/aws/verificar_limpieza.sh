#!/usr/bin/env bash
# EN TU MAC, después de terraform destroy. Lista recursos que aún podrían cobrar (us-east-1).
export AWS_REGION=us-east-1 AWS_DEFAULT_REGION=us-east-1 AWS_PAGER=""
echo "== EC2 (no terminadas) =="; aws ec2 describe-instances --query 'Reservations[].Instances[?State.Name!=`terminated`].[InstanceId,State.Name]' --output text
echo "== Volúmenes EBS =="; aws ec2 describe-volumes --query 'Volumes[].VolumeId' --output text
echo "== Elastic IPs =="; aws ec2 describe-addresses --query 'Addresses[].PublicIp' --output text
echo "== NAT Gateways =="; aws ec2 describe-nat-gateways --filter Name=state,Values=available,pending --query 'NatGateways[].NatGatewayId' --output text
echo "== RDS =="; aws rds describe-db-instances --query 'DBInstances[].DBInstanceIdentifier' --output text
echo "== RDS snapshots manuales =="; aws rds describe-db-snapshots --snapshot-type manual --query 'DBSnapshots[].DBSnapshotIdentifier' --output text
echo "== DocumentDB =="; aws docdb describe-db-clusters --query 'DBClusters[].DBClusterIdentifier' --output text
echo "== Redshift Serverless workgroups =="; aws redshift-serverless list-workgroups --query 'workgroups[].workgroupName' --output text
echo "== Redshift Serverless namespaces =="; aws redshift-serverless list-namespaces --query 'namespaces[].namespaceName' --output text
echo "== Buckets S3 taller-dw =="; aws s3api list-buckets --query 'Buckets[?starts_with(Name,`taller-dw`)].Name' --output text
echo "== Secretos (programados a borrar no cobran tras borrado) =="; aws secretsmanager list-secrets --query 'SecretList[].Name' --output text
echo "Todo vacío = nada facturable. Revisa además QuickSight (Unsubscribe) y Billing."
