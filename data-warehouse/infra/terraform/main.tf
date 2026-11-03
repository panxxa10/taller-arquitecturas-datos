terraform {
  required_version = ">= 1.6"
  required_providers {
    aws    = { source = "hashicorp/aws", version = ">= 5.60" }
    random = { source = "hashicorp/random", version = ">= 3.6" }
  }
}

provider "aws" {
  region  = var.region
  profile = var.profile
  default_tags {
    tags = { Proyecto = "taller-dw", Entrega = "arquitecturas-de-datos" }
  }
}

data "aws_caller_identity" "me" {}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

data "aws_ssm_parameter" "al2023" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

resource "random_id" "sfx" {
  byte_length = 3
}

# ---------------------------------------------------------------- S3 (privado)
resource "aws_s3_bucket" "dw" {
  bucket        = "taller-dw-${data.aws_caller_identity.me.account_id}-${random_id.sfx.hex}"
  force_destroy = true # para poder eliminarlo en el cierre aunque tenga objetos
}

resource "aws_s3_bucket_public_access_block" "dw" {
  bucket                  = aws_s3_bucket.dw.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "dw" {
  bucket = aws_s3_bucket.dw.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

# Versionado: un objeto sobrescrito por error no se pierde en silencio
resource "aws_s3_bucket_versioning" "dw" {
  bucket = aws_s3_bucket.dw.id
  versioning_configuration { status = "Enabled" }
}

# ---------------------------------------------------------------- Seguridad de red
resource "aws_key_pair" "airflow" {
  key_name   = "taller-dw-airflow"
  public_key = file(pathexpand(var.public_key_path))
}

resource "aws_security_group" "ec2" {
  name        = "taller-dw-ec2"
  description = "EC2 Airflow+dbt: SSH solo desde mi IP (la UI de Airflow va por tunel SSH)"
  vpc_id      = data.aws_vpc.default.id
  ingress {
    description = "SSH desde mi IP"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.my_ip_cidr]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

locals {
  db_ports = { rds = 3306, docdb = 27017, redshift = 5439 }
}

resource "aws_security_group" "db" {
  for_each    = local.db_ports
  name        = "taller-dw-${each.key}"
  description = "${each.key}: ${each.value} solo desde la EC2 de Airflow"
  vpc_id      = data.aws_vpc.default.id
  ingress {
    from_port       = each.value
    to_port         = each.value
    protocol        = "tcp"
    security_groups = [aws_security_group.ec2.id]
  }
  # Solo Redshift: QuickSight entra por su conexión de VPC (interfaces de red con este grupo de seguridad)
  dynamic "ingress" {
    for_each = each.key == "redshift" ? [1] : []
    content {
      description     = "QuickSight (conexion de VPC)"
      from_port       = 5439
      to_port         = 5439
      protocol        = "tcp"
      security_groups = [aws_security_group.quicksight.id]
    }
  }
}

# ---------------------------------------------------------------- QuickSight: conexión privada a Redshift
# Grupo de seguridad de las interfaces de red de QuickSight: solo habla con Redshift por 5439.
resource "aws_security_group" "quicksight" {
  name        = "taller-dw-quicksight"
  description = "QuickSight: conexion de VPC hacia Redshift (5439)"
  vpc_id      = data.aws_vpc.default.id
}

resource "aws_vpc_security_group_egress_rule" "quicksight_to_redshift" {
  security_group_id            = aws_security_group.quicksight.id
  referenced_security_group_id = aws_security_group.db["redshift"].id
  ip_protocol                  = "tcp"
  from_port                    = 5439
  to_port                      = 5439
  description                  = "Redshift"
}

resource "aws_vpc_security_group_ingress_rule" "quicksight_from_redshift" {
  security_group_id            = aws_security_group.quicksight.id
  referenced_security_group_id = aws_security_group.db["redshift"].id
  ip_protocol                  = "tcp"
  from_port                    = 0
  to_port                      = 65535
  description                  = "Respuestas de Redshift"
}

# Rol que QuickSight asume para crear/administrar las interfaces de red de la conexión de VPC
resource "aws_iam_role" "quicksight_vpc" {
  name = "taller-dw-quicksight-vpc"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "quicksight.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "quicksight_vpc" {
  name = "vpc-connection"
  role = aws_iam_role.quicksight_vpc.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "ec2:CreateNetworkInterface", "ec2:ModifyNetworkInterfaceAttribute", "ec2:DeleteNetworkInterface",
        "ec2:DescribeSubnets", "ec2:DescribeSecurityGroups", "ec2:DescribeNetworkInterfaces"
      ]
      Resource = "*"
    }]
  })
}

# ---------------------------------------------------------------- RDS for MySQL
resource "aws_db_subnet_group" "rds" {
  name       = "taller-dw-rds"
  subnet_ids = data.aws_subnets.default.ids
}

resource "aws_db_instance" "mysql" {
  identifier                  = "taller-dw-mysql"
  engine                      = "mysql"
  engine_version              = "8.0"
  instance_class              = "db.t4g.micro"
  allocated_storage           = 20
  storage_type                = "gp3"
  storage_encrypted           = true
  db_name                     = "dw_source"
  username                    = "admin"
  manage_master_user_password = true # secreto en Secrets Manager; no queda en el tfstate
  db_subnet_group_name        = aws_db_subnet_group.rds.name
  vpc_security_group_ids      = [aws_security_group.db["rds"].id]
  publicly_accessible         = false
  backup_retention_period     = 0
  skip_final_snapshot         = true
  deletion_protection         = false
  apply_immediately           = true
}

# ---------------------------------------------------------------- DocumentDB (Serverless)
resource "aws_docdb_subnet_group" "this" {
  name       = "taller-dw-docdb"
  subnet_ids = data.aws_subnets.default.ids
}

resource "aws_docdb_cluster" "this" {
  cluster_identifier          = "taller-dw-docdb"
  engine                      = "docdb"
  engine_version              = "5.0.0"
  master_username             = "docdbadmin"
  manage_master_user_password = true
  db_subnet_group_name        = aws_docdb_subnet_group.this.name
  vpc_security_group_ids      = [aws_security_group.db["docdb"].id]
  storage_encrypted           = true
  backup_retention_period     = 1
  skip_final_snapshot         = true
  deletion_protection         = false
  serverless_v2_scaling_configuration {
    min_capacity = 0.5
    max_capacity = 1
  }
}

resource "aws_docdb_cluster_instance" "this" {
  identifier         = "taller-dw-docdb-1"
  cluster_identifier = aws_docdb_cluster.this.id
  instance_class     = "db.serverless"
}

# ---------------------------------------------------------------- Redshift Serverless
# Rol que Redshift asume para leer S3 (COPY)
resource "aws_iam_role" "redshift_s3" {
  name = "taller-dw-redshift-s3"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "redshift.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "redshift_s3" {
  name = "leer-bucket-dw"
  role = aws_iam_role.redshift_s3.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:GetBucketLocation", "s3:ListBucket"]
      Resource = [aws_s3_bucket.dw.arn, "${aws_s3_bucket.dw.arn}/*"]
    }]
  })
}

resource "aws_redshiftserverless_namespace" "this" {
  namespace_name        = "taller-dw"
  db_name               = "dw"
  admin_username        = "dwadmin"
  manage_admin_password = true # contraseña en Secrets Manager
  iam_roles             = [aws_iam_role.redshift_s3.arn]
}

resource "aws_redshiftserverless_workgroup" "this" {
  namespace_name      = aws_redshiftserverless_namespace.this.namespace_name
  workgroup_name      = "taller-dw"
  base_capacity       = 4 # mínimo permitido (RPU)
  publicly_accessible = false
  subnet_ids          = data.aws_subnets.default.ids
  security_group_ids  = [aws_security_group.db["redshift"].id]
}

# ---------------------------------------------------------------- EC2: Airflow + dbt
resource "aws_iam_role" "ec2" {
  name = "taller-dw-ec2"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# Mínimo para el pipeline: S3 del DW, leer SOLO los 3 secretos de las bases y la Data API
resource "aws_iam_role_policy" "ec2" {
  name = "pipeline-dw"
  role = aws_iam_role.ec2.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = aws_s3_bucket.dw.arn
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject"] # sin DeleteObject: no se borran lotes
        Resource = "${aws_s3_bucket.dw.arn}/*"
      },
      {
        Effect = "Allow"
        Action = ["secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"]
        Resource = [
          aws_db_instance.mysql.master_user_secret[0].secret_arn,
          aws_docdb_cluster.this.master_user_secret[0].secret_arn,
          aws_redshiftserverless_namespace.this.admin_password_secret_arn,
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["redshift-serverless:GetWorkgroup", "redshift-serverless:GetNamespace"]
        Resource = "*"
      },
      {
        # API de datos de Redshift: enviar SQL solo al workgroup del taller
        Effect   = "Allow"
        Action   = ["redshift-data:ExecuteStatement", "redshift-data:BatchExecuteStatement"]
        Resource = aws_redshiftserverless_workgroup.this.arn
      },
      {
        # Consultar el estado y los resultados de las sentencias propias
        Effect   = "Allow"
        Action   = ["redshift-data:DescribeStatement", "redshift-data:GetStatementResult", "redshift-data:CancelStatement", "redshift-data:ListStatements"]
        Resource = "*"
      },
    ]
  })
}

resource "aws_iam_instance_profile" "ec2" {
  name = "taller-dw-ec2"
  role = aws_iam_role.ec2.name
}

resource "aws_instance" "airflow" {
  ami                         = data.aws_ssm_parameter.al2023.value
  instance_type               = var.ec2_type # Airflow + dbt necesitan ~8 GB de RAM
  subnet_id                   = data.aws_subnets.default.ids[0]
  vpc_security_group_ids      = [aws_security_group.ec2.id]
  key_name                    = aws_key_pair.airflow.key_name
  iam_instance_profile        = aws_iam_instance_profile.ec2.name
  associate_public_ip_address = true
  user_data                   = file("${path.module}/user_data.sh")
  metadata_options {
    http_tokens                 = "required" # IMDSv2
    http_put_response_hop_limit = 2          # permite que los contenedores Docker (Airflow) usen el rol de la EC2
  }
  root_block_device {
    volume_size = 30
    volume_type = "gp3"
    encrypted   = true
  }
  tags = { Name = "taller-dw-airflow" }
}
