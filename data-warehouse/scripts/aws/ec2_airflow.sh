#!/usr/bin/env bash
# Ejecutar EN LA EC2, desde ~/data-warehouse.
#   ./scripts/aws/ec2_airflow.sh up                       construye e inicia Airflow (y activa el DAG)
#   ./scripts/aws/ec2_airflow.sh trigger [lote] [etapa]   dispara una corrida (lote AAAAMMDDTHHMMSS opcional; etapa de fallo opcional)
#   ./scripts/aws/ec2_airflow.sh status                   últimas corridas y estado de sus tareas
#   ./scripts/aws/ec2_airflow.sh logs <tarea> [corrida]   log de una tarea
#   ./scripts/aws/ec2_airflow.sh down                     detiene Airflow
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
COMPOSE=(docker compose -f docker/docker-compose.aws.yml --env-file docker/.env.airflow)
DAG=dw_pipeline

ensure_env() {
  [ -f .env.aws ] || { echo "Falta .env.aws: corre sync_to_ec2.sh desde tu Mac"; exit 1; }
  if [ ! -f docker/.env.airflow ]; then
    umask 077
    PASS="$(openssl rand -hex 12)"
    cat > docker/.env.airflow <<ENV
POSTGRES_PASSWORD=$(openssl rand -hex 16)
AIRFLOW_ADMIN_PASSWORD=$PASS
AIRFLOW__CORE__FERNET_KEY=$(openssl rand -base64 32 | tr '+/' '-_')
AIRFLOW__WEBSERVER__SECRET_KEY=$(openssl rand -hex 32)
AIRFLOW_UID=$(id -u)
ENV
    echo "Secretos de Airflow generados en docker/.env.airflow (solo existen en la EC2; no los compartas ni los subas)."
    echo "Usuario de la interfaz: airflow   Contraseña: $PASS"
  fi
}

in_scheduler() { "${COMPOSE[@]}" exec -T -e PYTHONWARNINGS=ignore -e AIRFLOW__LOGGING__LOGGING_LEVEL=ERROR airflow-scheduler "$@"; }
last_run() { in_scheduler airflow dags list-runs -d "$DAG" -o plain 2>/dev/null | grep -o "\(manual\|scheduled\)__[^ ]*" | head -1; }

case "${1:-}" in
  up)
    ensure_env
    "${COMPOSE[@]}" build
    "${COMPOSE[@]}" up -d
    echo "Esperando al scheduler..."
    for _ in $(seq 1 60); do in_scheduler airflow dags list >/dev/null 2>&1 && break; sleep 5; done
    for _ in $(seq 1 30); do in_scheduler airflow dags list 2>/dev/null | grep -q "$DAG" && break; sleep 5; done
    in_scheduler airflow dags unpause "$DAG"
    in_scheduler airflow dags list-import-errors || true
    echo "Listo. Dispara una corrida con: ./scripts/aws/ec2_airflow.sh trigger"
    ;;
  trigger)
    BATCH="${2:-}"; FAIL="${3:-none}"
    in_scheduler airflow dags trigger "$DAG" --conf "{\"batch_id\": \"$BATCH\", \"inject_failure\": \"$FAIL\"}"
    ;;
  status)
    in_scheduler airflow dags list-runs -d "$DAG" -o table 2>/dev/null | grep -v "^$" | head -12
    RUN="$(last_run)"
    [ -n "$RUN" ] && { echo; echo "Tareas de la última corrida ($RUN):"; in_scheduler airflow tasks states-for-dag-run "$DAG" "$RUN" -o table 2>/dev/null | grep -v "^$"; }
    ;;
  logs)
    TASK="${2:?uso: logs <tarea> [corrida]}"
    RUN="${3:-$(last_run)}"
    in_scheduler bash -c "cat '/opt/airflow/logs/dag_id=$DAG/run_id=$RUN/task_id=$TASK/'attempt=*.log" | grep -a "dag_run=\|ERROR\|Error\|FALLO\|Done\.\|Completed\|Marking" | tail -60
    ;;
  down)
    "${COMPOSE[@]}" down
    ;;
  *)
    sed -n 2,8p "$0"; exit 1;;
esac
