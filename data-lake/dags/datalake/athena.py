import logging
import time

import boto3

log = logging.getLogger(__name__)

CHECKS = {
    "news_id_unico": """
        SELECT COUNT(*) - COUNT(DISTINCT news_id) AS fallas FROM silver_news""",
    "campos_obligatorios": """
        SELECT COUNT(*) AS fallas FROM silver_news
        WHERE news_id IS NULL OR title IS NULL OR title = '' OR link IS NULL OR medio IS NULL""",
    "fechas_y_enlaces_validos": """
        SELECT COUNT(*) AS fallas FROM silver_news
        WHERE NOT regexp_like(link, '^https?://[^/]+')
           OR (pub_date <> 'unknown' AND (pub_ts_utc IS NULL OR date_format(pub_ts_utc, '%Y-%m-%d') <> pub_date))
           OR (pub_date = 'unknown' AND pub_ts_utc IS NOT NULL)""",
    "entidades_sin_noticia": """
        SELECT COUNT(*) AS fallas FROM silver_news_entities e
        LEFT JOIN silver_news n ON e.news_id = n.news_id AND e.text_hash = n.content_hash
        WHERE n.news_id IS NULL""",
    "frases_sin_noticia": """
        SELECT COUNT(*) AS fallas FROM silver_news_key_phrases p
        LEFT JOIN silver_news n ON p.news_id = n.news_id AND p.text_hash = n.content_hash
        WHERE n.news_id IS NULL""",
    "conciliacion_noticias_gold_silver": """
        WITH s AS (
            SELECT n.pub_date, sec AS seccion, COUNT(DISTINCT n.news_id) AS c
            FROM silver_news n CROSS JOIN UNNEST(n.secciones) AS t(sec)
            GROUP BY 1, 2),
        g AS (SELECT pub_date, seccion, SUM(news_count) AS c FROM gold_news_daily GROUP BY 1, 2)
        SELECT COUNT(*) AS fallas FROM s FULL OUTER JOIN g ON s.pub_date = g.pub_date AND s.seccion = g.seccion
        WHERE COALESCE(s.c, -1) <> COALESCE(g.c, -1)""",
    "conciliacion_entidades_gold_silver": """
        WITH s AS (
            SELECT n.pub_date, e.entity_type, e.entity_norm, COUNT(DISTINCT e.news_id) AS c
            FROM silver_news_entities e JOIN silver_news n ON e.news_id = n.news_id
            GROUP BY 1, 2, 3),
        g AS (SELECT pub_date, entity_type, entity_norm, news_count AS c FROM gold_entity_daily)
        SELECT COUNT(*) AS fallas FROM s FULL OUTER JOIN g
            ON s.pub_date = g.pub_date AND s.entity_type = g.entity_type AND s.entity_norm = g.entity_norm
        WHERE COALESCE(s.c, -1) <> COALESCE(g.c, -1)""",
}


def run_query(cfg: dict, sql: str, client=None, timeout: int = 180) -> dict:
    client = client or boto3.client("athena", region_name=cfg["region"])
    qid = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": cfg["glue_database"]},
        ResultConfiguration={"OutputLocation": f"s3://{cfg['bucket']}/{cfg['athena_output_prefix']}"},
        WorkGroup=cfg.get("athena_workgroup", "primary"),
    )["QueryExecutionId"]

    start = time.time()
    while True:
        ex = client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = ex["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        if time.time() - start > timeout:
            client.stop_query_execution(QueryExecutionId=qid)
            raise TimeoutError(f"Consulta {qid} superó {timeout}s")
        time.sleep(1)
    if state != "SUCCEEDED":
        raise RuntimeError(f"Consulta {qid} {state}: {ex['Status'].get('StateChangeReason')}")

    rows = []
    for page in client.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
        for r in page["ResultSet"]["Rows"]:
            rows.append([c.get("VarCharValue") for c in r["Data"]])
    header, data = (rows[0], rows[1:]) if rows else ([], [])
    return {"query_id": qid, "columns": header, "rows": data,
            "bytes_scanned": ex["Statistics"].get("DataScannedInBytes", 0)}


def run_checks(cfg: dict, client=None) -> dict:
    results = {}
    for name, sql in CHECKS.items():
        res = run_query(cfg, sql, client)
        fallas = int(res["rows"][0][0]) if res["rows"] else -1
        results[name] = {"fallas": fallas, "ok": fallas == 0, "query_id": res["query_id"],
                         "bytes_scanned": res["bytes_scanned"]}
        log.info("check=%s fallas=%s query_id=%s", name, fallas, res["query_id"])
    return results
