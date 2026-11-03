#!/usr/bin/env bash
# Ejecutar EN LA EC2, desde ~/data-warehouse. Instala dbt para Redshift en su propio entorno virtual.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
[ -d .venv-dbt ] || python3.11 -m venv .venv-dbt
.venv-dbt/bin/pip install -q --upgrade pip
.venv-dbt/bin/pip install -q "dbt-core==1.9.4" "dbt-redshift~=1.9.0" "boto3>=1.35"
.venv-dbt/bin/dbt --version
