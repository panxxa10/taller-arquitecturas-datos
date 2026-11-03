"""DDL y sentencias de carga de Redshift generadas desde las definiciones de entidades."""

from ecommerce_dw.ingestion.entities import EXTRACTION_MODE, MONGO_CSV_COLUMNS, MYSQL_CSV_COLUMNS

RAW_SCHEMA = '"raw"'  # RAW es palabra reservada en Redshift: siempre entre comillas
PREFIX = {"mysql": "mysql", "mongodb": "mongo"}
META_RAW = ["_batch_id VARCHAR(32)", "_extracted_at TIMESTAMPTZ", "_loaded_at TIMESTAMPTZ", "_source VARCHAR(64)"]

CONTROL_DDL = f"""CREATE TABLE IF NOT EXISTS {RAW_SCHEMA}.batch_control (
    batch_id VARCHAR(32), source VARCHAR(16), entity VARCHAR(40), batch_type VARCHAR(20),
    extracted_at TIMESTAMPTZ, rows_expected BIGINT, rows_loaded BIGINT, status VARCHAR(20),
    manifest_key VARCHAR(400), run_id VARCHAR(200), loaded_at TIMESTAMPTZ
)"""


def lit(value):
    """Literal SQL seguro para valores que vienen de nuestros manifiestos."""
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def redshift_type(column, declared):
    if column == "document_json":
        return "VARCHAR(65535)"
    if declared == "VARCHAR":
        return "VARCHAR(1000)"
    return declared


def definitions():
    for source, columns in (("mysql", MYSQL_CSV_COLUMNS), ("mongodb", MONGO_CSV_COLUMNS)):
        for entity, cols in columns.items():
            yield source, entity, cols


def raw_table(source, entity):
    return f"{RAW_SCHEMA}.{PREFIX[source]}_{entity}"


def create_statements():
    statements = [f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA}", CONTROL_DDL]
    for source, entity, cols in definitions():
        body = ", ".join([f"{name} {redshift_type(name, kind)}" for name, kind in cols.items()] + META_RAW)
        statements.append(f"CREATE TABLE IF NOT EXISTS {raw_table(source, entity)} ({body})")
    return statements


def batch_statements(manifest, bucket, role_arn, region):
    """Sentencias (una transacción) que cargan un lote: COPY a una tabla temporal, reemplazo por llave e inscripción en batch_control."""
    source, entity, key = manifest["source"], manifest["entity"], manifest["key"]
    columns = next(c for s, e, c in definitions() if (s, e) == (source, entity))
    cols = list(columns)
    table = raw_table(source, entity)
    stage = f"stg_{PREFIX[source]}_{entity}"
    rows = manifest["rows"]
    source_label = f"{source}.{entity}"
    sqls = []

    if rows > 0:
        stage_cols = ", ".join([f"{name} {redshift_type(name, kind)}" for name, kind in columns.items()]
                               + ["_batch_id VARCHAR(32)", "_extracted_at VARCHAR(40)"])
        sqls.append(f"CREATE TEMP TABLE {stage} ({stage_cols})")
        for part in manifest["files"]:
            sqls.append(
                f"COPY {stage} FROM 's3://{bucket}/{part['key']}' IAM_ROLE '{role_arn}' "
                f"FORMAT AS CSV GZIP IGNOREHEADER 1 EMPTYASNULL TIMEFORMAT 'auto' REGION '{region}'"
            )

    if manifest["mode"] == "full":
        sqls.append(f"DELETE FROM {table}")
    elif rows > 0:
        sqls.append(f"DELETE FROM {table} WHERE {key} IN (SELECT {key} FROM {stage})")

    if rows > 0:
        select_cols = ", ".join(cols)
        sqls.append(
            f"INSERT INTO {table} ({select_cols}, _batch_id, _extracted_at, _loaded_at, _source) "
            f"SELECT {select_cols}, _batch_id, "
            f"CAST(REPLACE(REPLACE(_extracted_at, 'T', ' '), 'Z', '+00') AS TIMESTAMPTZ), CURRENT_TIMESTAMP, {lit(source_label)} "
            f"FROM {stage}"
        )

    sqls.append(
        f"DELETE FROM {RAW_SCHEMA}.batch_control WHERE batch_id = {lit(manifest['batch_id'])} "
        f"AND source = {lit(source)} AND entity = {lit(entity)}"
    )
    head = (f"{lit(manifest['batch_id'])}, {lit(source)}, {lit(entity)}, {lit(manifest['batch_type'])}, "
            f"CAST(REPLACE(REPLACE({lit(manifest['extracted_at'])}, 'T', ' '), 'Z', '+00') AS TIMESTAMPTZ), {int(rows)}")
    tail = f"{lit(manifest['files'][0]['key'] if manifest['files'] else None)}, {lit(manifest.get('run_id'))}, CURRENT_TIMESTAMP"
    if rows > 0:
        sqls.append(
            f"INSERT INTO {RAW_SCHEMA}.batch_control SELECT {head}, COUNT(*), "
            f"CASE WHEN COUNT(*) = {int(rows)} THEN 'loaded' ELSE 'count_mismatch' END, {tail} FROM {stage}"
        )
    else:
        sqls.append(f"INSERT INTO {RAW_SCHEMA}.batch_control SELECT {head}, 0, 'loaded', {tail}")
    return sqls
