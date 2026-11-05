import logging

import boto3
import pyarrow as pa

log = logging.getLogger(__name__)

PARQUET_IN = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
PARQUET_OUT = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
PARQUET_SERDE = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
PARTITION_KEYS = [{"Name": "pub_date", "Type": "string", "Comment": "Fecha de publicación UTC (YYYY-MM-DD) o 'unknown'"}]


def glue_type(t: pa.DataType) -> str:
    if pa.types.is_string(t):
        return "string"
    if pa.types.is_int32(t):
        return "int"
    if pa.types.is_int64(t):
        return "bigint"
    if pa.types.is_floating(t):
        return "double"
    if pa.types.is_timestamp(t):
        return "timestamp"
    if pa.types.is_boolean(t):
        return "boolean"
    if pa.types.is_list(t):
        return f"array<{glue_type(t.value_type)}>"
    raise ValueError(f"Tipo no soportado: {t}")


def storage(schema: pa.Schema, location: str) -> dict:
    return {
        "Columns": [{"Name": f.name, "Type": glue_type(f.type)} for f in schema],
        "Location": location,
        "InputFormat": PARQUET_IN,
        "OutputFormat": PARQUET_OUT,
        "SerdeInfo": {"SerializationLibrary": PARQUET_SERDE, "Parameters": {"serialization.format": "1"}},
    }


def ensure_database(glue, name: str):
    try:
        glue.get_database(Name=name)
    except glue.exceptions.EntityNotFoundException:
        glue.create_database(DatabaseInput={"Name": name, "Description": "Data lake de noticias RSS (Silver y Gold)"})


def upsert_table(glue, db: str, name: str, base: str, schema: pa.Schema):
    table_input = {
        "Name": name,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {"classification": "parquet", "EXTERNAL": "TRUE"},
        "StorageDescriptor": storage(schema, base),
        "PartitionKeys": PARTITION_KEYS,
    }
    try:
        glue.get_table(DatabaseName=db, Name=name)
        glue.update_table(DatabaseName=db, TableInput=table_input)
    except glue.exceptions.EntityNotFoundException:
        glue.create_table(DatabaseName=db, TableInput=table_input)


def existing_partitions(glue, db: str, table: str) -> dict[str, str]:
    out = {}
    for page in glue.get_paginator("get_partitions").paginate(DatabaseName=db, TableName=table):
        for p in page["Partitions"]:
            out[p["Values"][0]] = p["StorageDescriptor"]["Location"]
    return out


def sync_partitions(glue, db: str, table: str, schema: pa.Schema, desired: dict[str, str]) -> dict:
    current = existing_partitions(glue, db, table)
    to_create = [d for d in desired if d not in current]
    to_update = [d for d in desired if d in current and current[d] != desired[d]]
    to_delete = [d for d in current if d not in desired]

    for i in range(0, len(to_create), 100):
        glue.batch_create_partition(DatabaseName=db, TableName=table, PartitionInputList=[
            {"Values": [d], "StorageDescriptor": storage(schema, desired[d])} for d in to_create[i:i + 100]])
    for d in to_update:
        glue.update_partition(DatabaseName=db, TableName=table, PartitionValueList=[d],
                              PartitionInput={"Values": [d], "StorageDescriptor": storage(schema, desired[d])})
    for i in range(0, len(to_delete), 25):
        glue.batch_delete_partition(DatabaseName=db, TableName=table,
                                    PartitionsToDelete=[{"Values": [d]} for d in to_delete[i:i + 25]])
    return {"created": len(to_create), "updated": len(to_update), "deleted": len(to_delete), "total": len(desired)}


def sync_catalog(cfg: dict, tables: dict[str, dict], glue=None) -> dict:
    from datalake.s3io import check_fail_injection

    check_fail_injection(cfg, "catalog")
    glue = glue or boto3.client("glue", region_name=cfg["region"])
    db = cfg["glue_database"]
    ensure_database(glue, db)
    report = {}
    for name, spec in sorted(tables.items()):
        upsert_table(glue, db, name, spec["base"], spec["schema"])
        report[name] = sync_partitions(glue, db, name, spec["schema"], spec["partitions"])
        log.info("glue table=%s.%s partitions=%s", db, name, report[name])
    return report
