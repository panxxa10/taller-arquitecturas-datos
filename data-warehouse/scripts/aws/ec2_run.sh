#!/usr/bin/env bash
# Ejecutar EN LA EC2, desde ~/data-warehouse. Corre un módulo Python del proyecto con el entorno cargado.
#   ./scripts/aws/ec2_run.sh ecommerce_dw.ingestion.s3_extract --list
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
set -a; source .env.aws; set +a
export PYTHONPATH="$PWD/src"
module="$1"; shift
exec .venv/bin/python -m "$module" "$@"
