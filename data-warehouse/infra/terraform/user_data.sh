#!/bin/bash
# Instala Docker, Docker Compose, git y Python en la EC2 de Airflow + dbt
set -euxo pipefail
dnf install -y docker git python3.11 python3.11-pip mariadb105
systemctl enable --now docker
usermod -aG docker ec2-user
mkdir -p /usr/local/lib/docker/cli-plugins
curl -SL https://github.com/docker/compose/releases/download/v2.29.7/docker-compose-linux-x86_64 \
  -o /usr/local/lib/docker/cli-plugins/docker-compose
chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
echo "listo" > /var/log/taller-dw-bootstrap.ok
