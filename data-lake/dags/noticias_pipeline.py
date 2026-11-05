import logging
from datetime import datetime, timedelta, timezone

from airflow.sdk import Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowException

log = logging.getLogger(__name__)

TRANSIENT_HTTP = {429, 500, 502, 503, 504}


@dag(
    dag_id="noticias_datalake",
    schedule="0 */2 * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    params={
        "reprocess_run_id": Param(None, type=["null", "string"], description="run_id de Bronze a reprocesar. Vacío = nueva extracción"),
        "rebuild_from_bronze": Param(False, type="boolean", description="Reconstruir Silver desde todos los lotes de Bronze, sin consultar los RSS"),
    },
    tags=["datalake", "rss"],
)
def noticias_datalake():
    @task
    def validate_config() -> dict:
        import boto3
        from datalake.config import load_pipeline, load_sources

        cfg = load_pipeline()
        sources = load_sources()
        ids = [s["id"] for s in sources]
        if len(ids) != len(set(ids)):
            raise ValueError(f"IDs de fuente repetidos: {ids}")
        for s in sources:
            for field in ("id", "medio", "seccion", "url"):
                if not s.get(field):
                    raise ValueError(f"Fuente sin '{field}': {s}")
            if not s["url"].startswith(("http://", "https://")):
                raise ValueError(f"URL inválida: {s['url']}")
        boto3.client("s3", region_name=cfg["region"]).head_bucket(Bucket=cfg["bucket"])
        boto3.client("glue", region_name=cfg["region"]).get_databases(MaxResults=1)
        boto3.client("athena", region_name=cfg["region"]).get_work_group(WorkGroup=cfg.get("athena_workgroup", "primary"))
        boto3.client("comprehend", region_name=cfg["region"]).detect_dominant_language(Text="prueba de acceso al servicio")
        log.info("Configuración válida: bucket=%s fuentes=%s", cfg["bucket"], ids)
        return {"bucket": cfg["bucket"], "sources": ids}

    @task
    def resolve_batch() -> dict:
        from datalake.bronze import load_batch_manifests, make_batch_id
        from datalake.config import load_pipeline

        ctx = get_current_context()
        reprocess = (ctx["params"].get("reprocess_run_id") or "").strip()
        if ctx["params"].get("rebuild_from_bronze"):
            from datalake.silver import list_bronze_run_ids

            run_ids = list_bronze_run_ids(load_pipeline())
            if not run_ids:
                raise ValueError("Bronze está vacío, no hay nada que reconstruir")
            batch = {"batch_id": run_ids[-1], "mode": "rebuild"}
        elif reprocess:
            if not load_batch_manifests(load_pipeline(), reprocess):
                raise ValueError(f"No existe el lote {reprocess} en Bronze")
            batch = {"batch_id": reprocess, "mode": "reprocess"}
        else:
            batch = {"batch_id": make_batch_id(ctx["dag_run"].run_id), "mode": "new"}
        log.info("batch=%s mode=%s", batch["batch_id"], batch["mode"])
        return batch

    @task
    def list_sources() -> list[dict]:
        from datalake.config import load_sources

        return load_sources()

    @task(map_index_template="{{ source_label }}")
    def ingest(source: dict, batch: dict) -> dict:
        from datalake import s3io
        from datalake.bronze import HTTP_ERROR, NETWORK_ERROR, bronze_prefix, ingest_source
        from datalake.config import load_pipeline

        get_current_context()["source_label"] = source["id"]
        cfg = load_pipeline()
        if batch["mode"] != "new":
            key = bronze_prefix(source["id"], batch["batch_id"]) + "manifest.json"
            if not s3io.exists(cfg, key):
                log.warning("source=%s batch=%s sin manifiesto en Bronze", source["id"], batch["batch_id"])
                return {"source_id": source["id"], "run_id": batch["batch_id"], "status": "missing_in_bronze"}
            return s3io.get_json(cfg, key)

        manifest = ingest_source(cfg, source, batch["batch_id"])
        transient = manifest["status"] == NETWORK_ERROR or (
            manifest["status"] == HTTP_ERROR and manifest["http_status"] in TRANSIENT_HTTP
        )
        ti = get_current_context()["ti"]
        if transient and ti.try_number <= ti.max_tries:
            raise AirflowException(f"Fallo transitorio en {source['id']}: {manifest['error']}. Se reintentará")
        return manifest

    @task
    def register_ingestion(batch: dict, manifests: list[dict]) -> dict:
        from datalake import s3io
        from datalake.bronze import OK, batch_outcome
        from datalake.config import load_pipeline

        cfg = load_pipeline()
        manifests = list(manifests)
        outcome = batch_outcome(manifests, len(manifests))
        record = {
            "run_id": batch["batch_id"],
            "mode": batch["mode"],
            "airflow_run_id": get_current_context()["dag_run"].run_id,
            "registered_at_utc": datetime.now(timezone.utc).isoformat(),
            "ingestion_outcome": outcome,
            "sources": {m["source_id"]: {"status": m["status"], "items": m.get("item_count", 0)} for m in manifests},
        }
        attempt = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        s3io.put_json(cfg, f"control/runs/run_id={batch['batch_id']}/ingestion_{batch['mode']}_{attempt}.json", record)
        log.info("batch=%s outcome=%s detalle=%s", batch["batch_id"], outcome, record["sources"])
        if not any(m["status"] == OK for m in manifests):
            raise AirflowException(f"Lote {batch['batch_id']} fallido: ninguna fuente válida")
        return record

    @task
    def build_silver(batch: dict, ingestion: dict) -> dict:
        from datalake import s3io
        from datalake.config import load_pipeline
        from datalake.silver import build_silver as run_silver, list_bronze_run_ids

        cfg = load_pipeline()
        attempt = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        if batch["mode"] == "rebuild":
            report = run_silver(cfg, list_bronze_run_ids(cfg), from_scratch=True, attempt=attempt)
        else:
            report = run_silver(cfg, [batch["batch_id"]], from_scratch=False, attempt=attempt)
        report["batch_mode"] = batch["mode"]
        s3io.put_json(cfg, f"control/runs/run_id={batch['batch_id']}/silver_{batch['mode']}_{attempt}.json", report)
        log.info("batch=%s silver=%s", batch["batch_id"], {k: v for k, v in report.items() if k != "runs"})
        return report

    @task
    def build_gold(silver_report: dict) -> dict:
        from datalake.config import load_pipeline
        from datalake.gold import build_gold as run_gold

        report = run_gold(load_pipeline())
        log.info("gold=%s", report)
        return report

    @task
    def update_catalog(gold_report: dict) -> dict:
        from datalake.catalog import sync_catalog
        from datalake.config import load_pipeline
        from datalake.gold import published_tables

        cfg = load_pipeline()
        return sync_catalog(cfg, published_tables(cfg))

    @task
    def validate_with_athena(catalog_report: dict) -> dict:
        from datalake.athena import run_checks
        from datalake.config import load_pipeline

        results = run_checks(load_pipeline())
        failed = [k for k, v in results.items() if not v["ok"]]
        if failed:
            raise AirflowException(f"Validaciones fallidas en Athena: {failed} -> {results}")
        return results

    @task(trigger_rule="all_done")
    def finalize(batch: dict) -> dict:
        from datalake import s3io
        from datalake.config import load_pipeline

        ctx = get_current_context()
        ti = ctx["ti"]
        stages = ["register_ingestion", "build_silver", "build_gold", "update_catalog", "validate_with_athena"]
        results = {t: ti.xcom_pull(task_ids=t) for t in stages}
        missing = [t for t, v in results.items() if v is None]
        ingestion = results["register_ingestion"] or {}
        if missing:
            status = "failed"
        elif ingestion.get("ingestion_outcome") == "partial":
            status = "partial"
        else:
            status = "complete"
        record = {
            "run_id": batch["batch_id"],
            "mode": batch["mode"],
            "airflow_run_id": ctx["dag_run"].run_id,
            "status": status,
            "failed_or_skipped_stages": missing,
            "finished_at_utc": datetime.now(timezone.utc).isoformat(),
            "stages": results,
        }
        attempt = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        s3io.put_json(load_pipeline(), f"control/runs/run_id={batch['batch_id']}/final_{batch['mode']}_{attempt}.json", record)
        log.info("batch=%s estado_final=%s etapas_faltantes=%s", batch["batch_id"], status, missing)
        if missing:
            raise AirflowException(f"Ejecución {status}: etapas sin resultado {missing}")
        return {"run_id": batch["batch_id"], "status": status}

    cfg_ok = validate_config()
    batch = resolve_batch()
    sources = list_sources()
    cfg_ok >> [batch, sources]
    manifests = ingest.partial(batch=batch).expand(source=sources)
    ingestion = register_ingestion(batch, manifests)
    silver = build_silver(batch, ingestion)
    gold = build_gold(silver)
    catalog = update_catalog(gold)
    checks = validate_with_athena(catalog)
    final = finalize(batch)
    [ingestion, silver, gold, catalog, checks] >> final


noticias_datalake()
