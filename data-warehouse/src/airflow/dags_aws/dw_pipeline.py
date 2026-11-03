"""Pipeline del data warehouse en AWS: extracción a S3 -> carga a Redshift -> dbt -> pruebas -> registro del resultado.

Una sola corrida hace todo sin intervención manual. Cada tarea registra en su log el id de la corrida y del lote.
Parámetros al disparar (todos opcionales):
  batch_id        lote AAAAMMDDTHHMMSS; vacío = hora de inicio de la corrida. Repetir un lote confirmado no duplica datos.
  inject_failure  none | validate | extract | load | dbt_run | dbt_test  (fallo controlado para la evidencia)
"""

from datetime import datetime, timedelta, timezone

from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from airflow import DAG
from ecommerce_dw.orchestration import aws_tasks as t

# Plantillas Jinja: se evalúan en cada corrida. Sin parámetro, el lote es la hora real de inicio de la corrida (siempre posterior
# a los lotes anteriores, incluso en corridas programadas atrasadas); los reintentos de una misma corrida conservan el lote.
RUN = "{{ run_id }}"
BATCH = "{{ params.batch_id.strip() or (dag_run.start_date or logical_date).strftime('%Y%m%dT%H%M%S') }}"
FAIL = "{{ params.inject_failure }}"


def _dbt(command, selector=None):
    return {"command": command, "selector": selector, "run_id": RUN, "batch_id": BATCH, "inject_failure": FAIL}


def _record(**context):
    run = context["dag_run"]
    states = {ti.task_id: str(ti.state) for ti in run.get_task_instances() if ti.task_id != "record_result"}
    t.record_result(
        run_id=run.run_id,
        batch_id=context["templates_dict"]["batch_id"],
        dag_id=run.dag_id,
        started_at=(run.start_date or datetime.now(timezone.utc)).isoformat(),
        task_states=states,
    )


with DAG(
    dag_id="dw_pipeline",
    description="S3 -> Redshift -> dbt (raw, intermediate, quarantine, snapshots, marts) -> pruebas",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="0 11 * * *",  # 06:00 hora de Bogotá (UTC-5)
    catchup=False,
    is_paused_upon_creation=True,
    # Una corrida y una tarea a la vez: una sola sesión de dbt/Redshift, sin transformaciones concurrentes.
    max_active_runs=1,
    max_active_tasks=1,
    params={
        "batch_id": Param("", type="string", description="AAAAMMDDTHHMMSS; vacío = automático"),
        "inject_failure": Param("none", enum=list(t.STAGES), description="Fallo controlado para evidencia"),
    },
    # Reintentos acotados (2) con espera creciente; ninguna tarea corre más de 30 minutos.
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["data-warehouse", "aws"],
) as dag:
    common = {"run_id": RUN, "batch_id": BATCH, "inject_failure": FAIL}

    validate = PythonOperator(task_id="validate_connections", python_callable=t.validate_connections, op_kwargs=common)
    extract = PythonOperator(task_id="extract_to_s3", python_callable=t.extract_to_s3, op_kwargs=common)
    load = PythonOperator(task_id="load_redshift", python_callable=t.load_to_redshift, op_kwargs=common)
    deps = PythonOperator(task_id="dbt_deps", python_callable=t.run_dbt_task, op_kwargs=_dbt("deps"))
    seed = PythonOperator(task_id="dbt_seed", python_callable=t.run_dbt_task, op_kwargs=_dbt("seed"))
    intermediate = PythonOperator(task_id="dbt_run_intermediate", python_callable=t.run_dbt_task, op_kwargs=_dbt("run", "path:models/intermediate"))
    quarantine = PythonOperator(task_id="dbt_run_quarantine", python_callable=t.run_dbt_task, op_kwargs=_dbt("run", "path:models/quarantine"))
    snapshots = PythonOperator(task_id="dbt_snapshot", python_callable=t.run_dbt_task, op_kwargs=_dbt("snapshot"))
    marts = PythonOperator(task_id="dbt_run_marts", python_callable=t.run_dbt_task, op_kwargs=_dbt("run", "path:models/marts"))
    tests = PythonOperator(task_id="dbt_test", python_callable=t.run_dbt_task, op_kwargs=_dbt("test"))

    # Se ejecuta SIEMPRE (también si algo falló) para dejar el resultado en raw.pipeline_run_log; falla si hubo fallos.
    record = PythonOperator(
        task_id="record_result",
        python_callable=_record,
        templates_dict={"batch_id": BATCH},
        trigger_rule=TriggerRule.ALL_DONE,
        retries=0,
    )

    validate >> extract >> load >> deps >> seed >> intermediate >> quarantine >> snapshots >> marts >> tests >> record
