#!/usr/bin/env python3
"""Extracción por lotes de RDS MySQL y DocumentDB hacia S3 (capa raw).

Organización de objetos:
    raw/<fuente>/<entidad>/batch_id=<id>/part-0000.csv.gz   datos del lote (CSV gzip)
    raw/<fuente>/<entidad>/batch_id=<id>/_manifest.json     marca de confirmación del lote

* El manifiesto se escribe AL FINAL: un lote sin manifiesto es un intento incompleto y se rehace;
  un lote con manifiesto no se vuelve a consultar en las fuentes (idempotencia y reproceso).
* Las rutas incluyen batch_id, así un lote nunca pisa los archivos de otro (el bucket además tiene versionado).
* Modo "incremental": filas con llave > marca de agua del último lote confirmado (primer lote = carga inicial).
  Modo "full": instantánea completa en cada lote (dimensiones pequeñas; captura cambios con fecha retroactiva).
* batch_id = AAAAMMDDTHHMMSS (UTC), ordenable como texto; debe ser mayor que el del último lote confirmado.

CSV: lo entiende COPY de Redshift directamente (CSV + GZIP) y conserva las comillas de los documentos JSON.
"""

import argparse
import csv
import gzip
import hashlib
import json
import logging
import os
import re
import tempfile
from datetime import datetime, timezone

from .entities import EXTRACTION_MODE, MONGO_CSV_COLUMNS, MYSQL_CSV_COLUMNS

LOGGER = logging.getLogger(__name__)
BATCH_ID_PATTERN = re.compile(r"^\d{8}T\d{6}$")
EXTRACTOR_VERSION = "1.0"
META_COLUMNS = ["_batch_id", "_extracted_at"]
FETCH_SIZE = 1000


class BatchError(Exception):
    pass


def entities():
    """Lista de (fuente, entidad, columnas, modo, llave)."""
    result = []
    for source, definitions in (("mysql", MYSQL_CSV_COLUMNS), ("mongodb", MONGO_CSV_COLUMNS)):
        for name, columns in definitions.items():
            result.append((source, name, columns, EXTRACTION_MODE[(source, name)], next(iter(columns))))
    return result


def new_batch_id(now=None):
    return (now or datetime.now(timezone.utc)).strftime("%Y%m%dT%H%M%S")


def normalize_batch_id(batch_id):
    """Acepta el formato de Airflow (ts_nodash, con o sin zona) y valida el patrón."""
    batch_id = re.sub(r"(\+\d{4}|Z)$", "", batch_id)
    if not BATCH_ID_PATTERN.match(batch_id):
        raise BatchError(f"batch_id inválido '{batch_id}': usa AAAAMMDDTHHMMSS")
    return batch_id


def _csv_value(value):
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


# ---------------------------------------------------------------- lectura de las fuentes
def fetch_mysql(table, columns, key, watermark):
    from .raw_sources import connect_mysql

    sql = f"SELECT {', '.join(columns)} FROM {table}"
    params = ()
    if watermark is not None:
        sql += f" WHERE {key} > %s"
        params = (watermark,)
    sql += f" ORDER BY {key}"
    connection = connect_mysql()
    try:
        cursor = connection.cursor()
        cursor.execute(sql, params)
        while batch := cursor.fetchmany(FETCH_SIZE):
            yield from batch
    finally:
        connection.close()


def fetch_mongo(collection, columns, key, watermark):
    from pymongo import MongoClient

    from ecommerce_dw.aws_config import get_mongodb_uri

    from .raw_sources import flatten_mongo

    query = {} if watermark is None else {key: {"$gt": watermark}}
    client = MongoClient(get_mongodb_uri(), tz_aware=True)
    try:
        database = client[os.getenv("MONGO_DATABASE", "ecommerce")]
        for document in database[collection].find(query).sort(key, 1).batch_size(FETCH_SIZE):
            _, values = flatten_mongo(collection, document, None)
            yield values[:-2]  # sin _loaded_at ni _source: esos metadatos ahora son del lote
    finally:
        client.close()


def fetch_rows(source, name, columns, key, watermark):
    fetcher = fetch_mysql if source == "mysql" else fetch_mongo
    return fetcher(name, list(columns), key, watermark)


# ---------------------------------------------------------------- S3
def batch_prefix(source, name, batch_id):
    return f"raw/{source}/{name}/batch_id={batch_id}/"


def _read_manifest(s3, bucket, prefix):
    try:
        body = s3.get_object(Bucket=bucket, Key=prefix + "_manifest.json")["Body"].read()
    except s3.exceptions.NoSuchKey:
        return None
    return json.loads(body)


def committed_batches(s3, bucket, source, name):
    """Manifiestos de los lotes confirmados de una entidad, del más antiguo al más reciente."""
    root = f"raw/{source}/{name}/"
    manifests = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=root, Delimiter="/"):
        for common in page.get("CommonPrefixes", []):
            manifest = _read_manifest(s3, bucket, common["Prefix"])
            if manifest:
                manifests.append(manifest)
    return sorted(manifests, key=lambda manifest: manifest["batch_id"])


def _write_part(path, header, rows, key_index):
    """Escribe el CSV gzip y devuelve (filas, valor máximo numérico de la llave)."""
    count, high = 0, None
    with gzip.open(path, "wt", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(header)
        for row in rows:
            writer.writerow([_csv_value(value) for value in row])
            value = row[key_index]
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                high = value if high is None else max(high, value)
            count += 1
    return count, high


def _sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def extract_entity(s3, bucket, source, name, columns, mode, key, batch_id, run_id=None, now=None):
    """Extrae una entidad para un lote. Devuelve su manifiesto (con reused=True si ya existía)."""
    prefix = batch_prefix(source, name, batch_id)
    existing = _read_manifest(s3, bucket, prefix)
    if existing:
        LOGGER.info("Lote %s de %s.%s ya confirmado (%s filas): se reutiliza sin consultar la fuente",
                    batch_id, source, name, existing["rows"])
        return {**existing, "reused": True}

    previous = committed_batches(s3, bucket, source, name)
    if previous and batch_id <= previous[-1]["batch_id"]:
        raise BatchError(f"{source}.{name}: batch_id {batch_id} no es posterior al último lote confirmado "
                         f"({previous[-1]['batch_id']}); usa un identificador nuevo")

    watermark_from = None
    if mode == "incremental" and previous:
        watermark_from = previous[-1]["watermark_to"]
    batch_type = "initial" if not previous else ("full_snapshot" if mode == "full" else "incremental")

    extracted_at = (now or datetime.now(timezone.utc)).strftime("%Y-%m-%dT%H:%M:%SZ")
    header = list(columns) + META_COLUMNS
    rows = ((*row, batch_id, extracted_at) for row in fetch_rows(source, name, columns, key, watermark_from))

    files, row_count, high = [], 0, None
    with tempfile.TemporaryDirectory() as directory:
        path = os.path.join(directory, "part-0000.csv.gz")
        row_count, high = _write_part(path, header, rows, list(columns).index(key))
        if row_count:
            part_key = prefix + "part-0000.csv.gz"
            s3.upload_file(path, bucket, part_key)
            files.append({"key": part_key, "bytes": os.path.getsize(path), "sha256": _sha256(path)})

    manifest = {
        "batch_id": batch_id,
        "source": source,
        "entity": name,
        "batch_type": batch_type,
        "mode": mode,
        "extracted_at": extracted_at,
        "rows": row_count,
        "key": key,
        "watermark_from": watermark_from,
        "watermark_to": high if high is not None else watermark_from,
        "files": files,
        "run_id": run_id,
        "extractor_version": EXTRACTOR_VERSION,
        "status": "committed",
    }
    # El manifiesto se escribe al final: es lo que confirma el lote.
    s3.put_object(Bucket=bucket, Key=prefix + "_manifest.json",
                  Body=json.dumps(manifest, indent=2).encode("utf-8"), ContentType="application/json")
    LOGGER.info("Lote %s de %s.%s (%s): %s filas", batch_id, source, name, batch_type, row_count)
    return {**manifest, "reused": False}


def extract_batch(batch_id=None, only=None, run_id=None, bucket=None, s3=None):
    """Extrae todas las entidades (o las de `only`) para un lote y devuelve sus manifiestos."""
    import boto3

    batch_id = normalize_batch_id(batch_id) if batch_id else new_batch_id()
    bucket = bucket or os.environ["S3_BUCKET"]
    s3 = s3 or boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))
    results = []
    for source, name, columns, mode, key in entities():
        if only and name not in only:
            continue
        results.append(extract_entity(s3, bucket, source, name, columns, mode, key, batch_id, run_id))
    return results


def print_summary(manifests):
    print(f"{'fuente':<9}{'entidad':<19}{'lote':<17}{'tipo':<14}{'filas':>6}  {'marca de agua (desde → hasta)':<32}reutilizado")
    for m in manifests:
        span = f"{m['watermark_from']} → {m['watermark_to']}" if m["mode"] == "incremental" else "(instantánea completa)"
        print(f"{m['source']:<9}{m['entity']:<19}{m['batch_id']:<17}{m['batch_type']:<14}{m['rows']:>6}  {span:<32}{'sí' if m.get('reused') else 'no'}")


def list_batches(bucket=None, s3=None):
    import boto3

    bucket = bucket or os.environ["S3_BUCKET"]
    s3 = s3 or boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))
    manifests = []
    for source, name, *_ in entities():
        manifests.extend(committed_batches(s3, bucket, source, name))
    manifests.sort(key=lambda m: (m["batch_id"], m["source"], m["entity"]))
    return manifests


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--batch-id", help="AAAAMMDDTHHMMSS; por defecto, la hora actual UTC. Repetir un lote confirmado no consulta las fuentes.")
    parser.add_argument("--entities", help="Lista separada por comas (por defecto, todas)")
    parser.add_argument("--list", action="store_true", help="Lista los lotes confirmados en S3 y termina")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if args.list:
        print_summary([{**m, "reused": False} for m in list_batches()])
        return
    only = set(args.entities.split(",")) if args.entities else None
    print_summary(extract_batch(args.batch_id, only))


if __name__ == "__main__":
    main()
