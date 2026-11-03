#!/usr/bin/env bash
# Ejecutar EN LA EC2, desde ~/data-warehouse. Prepara el entorno, crea las fuentes en RDS/DocumentDB y las verifica.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

# 1) Certificado TLS (sirve para RDS y DocumentDB)
sudo mkdir -p /opt/certs
[ -f /opt/certs/global-bundle.pem ] || sudo curl -sSo /opt/certs/global-bundle.pem https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem

# 2) Entorno Python
[ -d .venv ] || python3.11 -m venv .venv
.venv/bin/pip install -q --upgrade pip
.venv/bin/pip install -q "Faker==37.0.0" "PyMySQL==1.1.2" "pymongo==4.16.0" "boto3>=1.35"

# 3) Variables no secretas; las contraseñas las lee el código de Secrets Manager
set -a; source .env.aws; set +a
export PYTHONPATH="$PWD/src"

# 4) Fuentes: esquema MySQL, datos sintéticos (mismos datos del taller local, seed 42) y verificación
.venv/bin/python -m ecommerce_dw.sources.create_source_schema
.venv/bin/python -m ecommerce_dw.sources.generate_data "$@"
.venv/bin/python -m ecommerce_dw.sources.verify_sources
