"""Lógica de las tareas del DAG `dw_pipeline` (Airflow en la EC2).

Los archivos de DAG solo declaran tareas y dependencias; todo el trabajo vive aquí para poder probarlo sin Airflow.
Cada tarea escribe en el log un prefijo `[dag_run=<run> lote=<batch> tarea=<tarea>]` que identifica corrida, lote y tarea.
"""

import json
import logging
import os
from datetime import datetime, timezone

from ecommerce_dw.ingestion.s3_extract import BatchError, extract_batch, normalize_batch_id
from ecommerce_dw.loading.ddl import RAW_SCHEMA, lit
from ecommerce_dw.loading.redshift_data_api import DataApi
from ecommerce_dw.loading.redshift_loader import init_schema, load_pending

LOGGER = logging.getLogger(__name__)

STAGES = ("none", "validate", "extract", "load", "dbt_run", "dbt_test")
REQUIRED_ENV = (
    "AWS_REGION", "S3_BUCKET", "MYSQL_HOST", "MYSQL_SECRET_ARN", "DOCDB_HOST", "DOCDB_SECRET_ARN",
    "REDSHIFT_HOST", "REDSHIFT_SECRET_ARN", "REDSHIFT_ROLE_ARN", "REDSHIFT_WORKGROUP", "REDSHIFT_DATABASE",
)
LOG_DDL = f"""
CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.pipeline_run_log (
    run_id VARCHAR(250), dag_id VARCHAR(100), batch_id VARCHAR(32), status VARCHAR(20),
    failed_tasks VARCHAR(1000), started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ, details VARCHAR(4000)
)
"""


def tag(run_id, batch_id, task):
    return f"[dag_run={run_id} lote={batch_id} tarea={task}]"


def resolve_batch_id(param_batch_id, logical_date_nodash):
    """El lote del parámetro, o uno derivado de la fecha lógica de la corrida (los reintentos reutilizan el mismo lote)."""
    return normalize_batch_id((param_batch_id or "").strip() or logical_date_nodash)


def maybe_fail(stage, inject_failure, run_id, batch_id):
    """Fallo controlado para la evidencia: se activa con el parámetro inject_failure del DAG."""
    if inject_failure not in STAGES:
        raise ValueError(f"inject_failure inválido '{inject_failure}': usa {STAGES}")
    if inject_failure == stage:
        raise RuntimeError(f"{tag(run_id, batch_id, stage)} FALLO CONTROLADO solicitado (inject_failure={stage})")


def validate_connections(run_id, batch_id, inject_failure="none"):
    """Verifica parámetros y conexiones a S3, MySQL, DocumentDB y Redshift antes de mover datos."""
    import boto3

    prefix = tag(run_id, batch_id, "validate_connections")
    maybe_fail("validate", inject_failure, run_id, batch_id)
    problems = [f"falta la variable {name}" for name in REQUIRED_ENV if not os.getenv(name)]
    if problems:
        raise RuntimeError(f"{prefix} parámetros incompletos: {problems}")
    normalize_batch_id(batch_id)

    checks = {}

    def check(name, fn):
        try:
            fn()
            checks[name] = "ok"
        except Exception as error:  # noqa: BLE001 - se informan todos los fallos juntos
            checks[name] = f"ERROR {type(error).__name__}: {error}"

    check("s3", lambda: boto3.client("s3", region_name=os.environ["AWS_REGION"]).head_bucket(Bucket=os.environ["S3_BUCKET"]))

    def mysql():
        from ecommerce_dw.sources.generate_data import connect_mysql

        connection = connect_mysql()
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
        finally:
            connection.close()

    def documentdb():
        from pymongo import MongoClient

        from ecommerce_dw.aws_config import get_mongodb_uri

        client = MongoClient(get_mongodb_uri(), serverSelectionTimeoutMS=15000)
        try:
            client.admin.command("ping")
        finally:
            client.close()

    check("mysql", mysql)
    check("documentdb", documentdb)
    check("redshift", lambda: DataApi().execute("SELECT 1"))
    for name, result in checks.items():
        LOGGER.info("%s conexión %s: %s", prefix, name, result)
    failed = {n: r for n, r in checks.items() if r != "ok"}
    if failed:
        raise RuntimeError(f"{prefix} conexiones fallidas: {failed}")
    return checks


def extract_to_s3(run_id, batch_id, inject_failure="none"):
    prefix = tag(run_id, batch_id, "extract_to_s3")
    maybe_fail("extract", inject_failure, run_id, batch_id)
    manifests = extract_batch(batch_id=batch_id, run_id=run_id)
    summary = [
        {"entity": f"{m['source']}.{m['entity']}", "type": m["batch_type"], "rows": m["rows"], "reused": m.get("reused", False)}
        for m in manifests
    ]
    for item in summary:
        LOGGER.info("%s %s", prefix, json.dumps(item, ensure_ascii=False))
    return summary


def load_to_redshift(run_id, batch_id, inject_failure="none"):
    prefix = tag(run_id, batch_id, "load_redshift")
    api = DataApi()
    init_schema(api)
    maybe_fail("load", inject_failure, run_id, batch_id)
    loaded = load_pending(api)
    LOGGER.info("%s %d lote(s)/entidad(es) cargados en esta corrida", prefix, len(loaded))
    control = api.execute(
        f"SELECT entity, rows_expected, rows_loaded, status FROM {RAW_SCHEMA}.batch_control WHERE batch_id = {lit(batch_id)} ORDER BY entity"
    )
    if not control:
        raise RuntimeError(f"{prefix} el lote no aparece en batch_control (¿falló la extracción?)")
    for entity, expected, loaded_rows, status in control:
        LOGGER.info("%s %s esperadas=%s cargadas=%s estado=%s", prefix, entity, expected, loaded_rows, status)
    return [{"entity": e, "rows": int(r), "status": s} for e, _, r, s in control]


def run_dbt_task(command, selector, run_id, batch_id, inject_failure="none"):
    from ecommerce_dw.transformations.dbt_runner import run_dbt

    task = f"dbt_{command}" + (f"[{selector}]" if selector else "")
    if command in ("run", "snapshot", "seed"):
        maybe_fail("dbt_run", inject_failure, run_id, batch_id)
    if command == "test":
        maybe_fail("dbt_test", inject_failure, run_id, batch_id)
    LOGGER.info("%s dbt %s %s", tag(run_id, batch_id, task), command, selector or "")
    run_dbt(command, selector)


def record_result(run_id, batch_id, dag_id, started_at, task_states):
    """Registra el resultado final de la corrida en raw.pipeline_run_log (se ejecuta aunque otras tareas fallen)."""
    prefix = tag(run_id, batch_id, "record_result")
    failed = sorted(t for t, state in task_states.items() if state in ("failed", "upstream_failed"))
    status = "failed" if failed else "success"
    api = DataApi()
    api.execute(LOG_DDL)
    details = json.dumps(task_states, ensure_ascii=False)[:4000]
    api.execute(
        f"INSERT INTO {RAW_SCHEMA}.pipeline_run_log VALUES ({lit(run_id)}, {lit(dag_id)}, {lit(batch_id)}, {lit(status)}, "
        f"{lit(','.join(failed)[:1000])}, {lit(started_at)}::timestamptz, {lit(datetime.now(timezone.utc).isoformat())}::timestamptz, {lit(details)})"
    )
    LOGGER.info("%s resultado=%s tareas_fallidas=%s", prefix, status, failed or "ninguna")
    if failed:
        raise RuntimeError(f"{prefix} la corrida terminó con fallos: {failed}")
    return status
