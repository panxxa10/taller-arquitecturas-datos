"""Informe de evidencias y conciliación del data warehouse en Redshift (solo lectura).

  ./scripts/aws/ec2_run.sh ecommerce_dw.orchestration.evidence
Imprime: resultado de corridas, lotes cargados, registros en cuarentena por motivo, duplicados, conciliación de filas
(raw = válidas + cuarentena), conciliación del indicador de ventas y comparación con el conteo de la fuente MySQL.
"""

from ecommerce_dw.loading.ddl import RAW_SCHEMA
from ecommerce_dw.loading.redshift_data_api import DataApi

# (tabla raw, llave, vista qa, tabla de cuarentena, vista valid)
ENTITIES = [
    ("mysql_customers", "customer_id", "int_mysql__customers", "quarantine_mysql__customers"),
    ("mysql_products", "product_id", "int_mysql__products", "quarantine_mysql__products"),
    ("mysql_orders", "order_id", "int_mysql__orders", "quarantine_mysql__orders"),
    ("mysql_order_items", "order_item_id", "int_mysql__order_items", "quarantine_mysql__order_items"),
    ("mongo_reviews", "review_id", "int_mongodb__reviews", "quarantine_mongodb__reviews"),
    ("mongo_navigation_events", "event_id", "int_mongodb__navigation_events", "quarantine_mongodb__navigation_events"),
]


def section(title, header, rows):
    print(f"\n== {title} ==")
    print("  " + " | ".join(header))
    for row in rows:
        print("  " + " | ".join("" if v is None else str(v) for v in row))
    if not rows:
        print("  (sin filas)")


def number(api, sql):
    return float(api.execute(sql)[0][0] or 0)


def mysql_counts():
    from ecommerce_dw.sources.generate_data import connect_mysql

    connection = connect_mysql()
    try:
        with connection.cursor() as cursor:
            result = {}
            for table in ("customers", "products", "orders", "order_items"):
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                result[f"mysql_{table}"] = int(cursor.fetchone()[0])
            return result
    finally:
        connection.close()


def report(api):
    section("Corridas del DAG (raw.pipeline_run_log)", ["corrida", "lote", "estado", "tareas_fallidas", "fin"],
            api.execute(f'SELECT run_id, batch_id, status, failed_tasks, finished_at FROM {RAW_SCHEMA}.pipeline_run_log ORDER BY finished_at'))

    section("Lotes cargados (raw.batch_control)", ["lote", "entidades", "filas_esperadas", "filas_cargadas", "estados"],
            api.execute(f"SELECT batch_id, COUNT(*), SUM(rows_expected), SUM(rows_loaded), LISTAGG(DISTINCT status, ',') FROM {RAW_SCHEMA}.batch_control GROUP BY batch_id ORDER BY batch_id"))

    union = " UNION ALL ".join(
        f"SELECT '{raw}' AS entidad, rejection_reason, _batch_id, COUNT(*) FROM quarantine.{quarantine} GROUP BY 2, 3"
        for raw, _, _, quarantine in ENTITIES)
    section("Registros en cuarentena (motivo y lote)", ["entidad", "motivo", "lote", "filas"],
            api.execute(f"SELECT * FROM ({union}) ORDER BY 1, 2, 3"))

    rows, failures = [], []
    for raw, key, qa, quarantine in ENTITIES:
        total = int(number(api, f"SELECT COUNT(*) FROM {RAW_SCHEMA}.{raw}"))
        distinct = int(number(api, f"SELECT COUNT(DISTINCT {key}) FROM {RAW_SCHEMA}.{raw}"))
        valid = int(number(api, f"SELECT COUNT(*) FROM intermediate.{qa}__valid"))
        rejected = int(number(api, f"SELECT COUNT(*) FROM quarantine.{quarantine}"))
        ok = total == distinct and total == valid + rejected
        rows.append((raw, total, total - distinct, valid, rejected, "OK" if ok else "NO CUADRA"))
        if not ok:
            failures.append(raw)
    section("Conciliación de filas: raw = válidas + cuarentena, sin duplicados",
            ["entidad", "raw", "duplicados", "válidas", "cuarentena", "resultado"], rows)

    raw_total = number(api, f"SELECT SUM(subtotal) FROM {RAW_SCHEMA}.mysql_order_items")
    valid_total = number(api, "SELECT SUM(subtotal) FROM intermediate.int_mysql__order_items__valid")
    rejected_total = number(api, "SELECT SUM(subtotal) FROM quarantine.quarantine_mysql__order_items")
    fact_total = number(api, "SELECT SUM(subtotal) FROM marts.fct_sales")
    fact_rows = int(number(api, "SELECT COUNT(*) FROM marts.fct_sales"))
    valid_rows = int(number(api, "SELECT COUNT(*) FROM intermediate.int_mysql__order_items__valid"))
    sales_ok = abs(raw_total - valid_total - rejected_total) < 0.01 and abs(valid_total - fact_total) < 0.01 and fact_rows == valid_rows
    section("Conciliación del indicador de ventas totales",
            ["concepto", "valor"],
            [("raw.mysql_order_items (suma subtotal)", f"{raw_total:.2f}"),
             ("válidas (intermediate)", f"{valid_total:.2f}"),
             ("cuarentena", f"{rejected_total:.2f}"),
             ("marts.fct_sales (indicador del dashboard)", f"{fact_total:.2f}"),
             ("filas fct_sales / válidas", f"{fact_rows} / {valid_rows}"),
             ("resultado", "OK: raw = válidas + cuarentena y fct_sales = válidas" if sales_ok else "NO CUADRA")])

    source_ok = True
    try:
        source = mysql_counts()
        comparison = []
        for table, count in source.items():
            loaded = int(number(api, f"SELECT COUNT(*) FROM {RAW_SCHEMA}.{table}"))
            same = count == loaded
            source_ok = source_ok and same
            comparison.append((table, count, loaded, "OK" if same else "DIFIERE (lote nuevo sin extraer/cargar)"))
        section("Fuente MySQL vs raw en Redshift (filas)", ["tabla", "MySQL", "Redshift raw", "resultado"], comparison)
    except Exception as error:  # noqa: BLE001 - la comparación con la fuente es complementaria
        print(f"\n(No se pudo consultar MySQL: {type(error).__name__}: {error})")

    ok = not failures and sales_ok and source_ok
    print("\nRESULTADO GLOBAL:", "TODO CUADRA" if ok else f"REVISAR (filas: {failures or 'ok'}, ventas: {'ok' if sales_ok else 'no cuadra'}, fuente: {'ok' if source_ok else 'difiere'})")
    return ok


if __name__ == "__main__":
    raise SystemExit(0 if report(DataApi()) else 1)
