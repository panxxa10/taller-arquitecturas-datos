#!/usr/bin/env bash
# Instala Docker y prepara Airflow 3.3.0 con Docker Compose en Ubuntu 24.04.
# Ejecutar desde la raíz del repo: bash infra/setup_ec2.sh
set -euo pipefail

AIRFLOW_VERSION=3.3.0
cd "$(dirname "$0")/.."

if ! command -v docker >/dev/null; then
  curl -fsSL https://get.docker.com | sudo sh
  sudo usermod -aG docker "$USER"
  echo "Docker instalado. Cierra la sesión SSH, vuelve a entrar y ejecuta este script otra vez."
  exit 0
fi

curl -LfO "https://airflow.apache.org/docs/apache-airflow/${AIRFLOW_VERSION}/docker-compose.yaml"
sed -i "s/AIRFLOW__CORE__LOAD_EXAMPLES: 'true'/AIRFLOW__CORE__LOAD_EXAMPLES: 'false'/" docker-compose.yaml

mkdir -p dags logs plugins config

if [ ! -f .env ]; then
  cp .env.example .env
  sed -i "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$(id -u)/" .env
  FERNET=$(python3 -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())")
  sed -i "s|^FERNET_KEY=.*|FERNET_KEY=${FERNET}|" .env
fi

docker compose up airflow-init
docker compose up -d
docker compose ps
