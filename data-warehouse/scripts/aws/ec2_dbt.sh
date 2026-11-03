#!/usr/bin/env bash
# Ejecutar EN LA EC2, desde ~/data-warehouse. Corre dbt contra Redshift con las credenciales de Secrets Manager.
#   ./scripts/aws/ec2_dbt.sh deps
#   ./scripts/aws/ec2_dbt.sh run path:models/intermediate
#   ./scripts/aws/ec2_dbt.sh test
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
set -a; source .env.aws; set +a
export PYTHONPATH="$PWD/src" DBT_TARGET=redshift
export DBT_EXECUTABLE="$PWD/.venv-dbt/bin/dbt" DBT_PROJECT_DIR="$PWD/src/dbt"
export REDSHIFT_HOST="${REDSHIFT_HOST:?falta REDSHIFT_HOST en .env.aws}"
exec .venv-dbt/bin/python -m ecommerce_dw.transformations.dbt_runner "$@"
