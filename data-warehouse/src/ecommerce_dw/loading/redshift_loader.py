#!/usr/bin/env python3
"""Carga a Redshift los lotes de S3 que aún no están registrados en raw.batch_control.

Cada lote/entidad se aplica en UNA transacción (COPY → reemplazo por llave → inscripción en batch_control), por lo que
repetir la carga de un lote no duplica filas y un fallo a la mitad no deja cambios parciales.
  --init     crea el esquema raw, las tablas y raw.batch_control
  --load     carga los lotes pendientes en orden de batch_id
  --status   muestra batch_control y los conteos de las tablas raw
"""

import argparse
import logging

from ecommerce_dw.aws_config import redshift_settings
from ecommerce_dw.ingestion.s3_extract import list_batches

from .ddl import RAW_SCHEMA, batch_statements, create_statements, definitions, raw_table
from .redshift_data_api import DataApi

LOGGER = logging.getLogger(__name__)


def init_schema(api):
    for statement in create_statements():
        api.execute(statement)
    LOGGER.info("Esquema raw y tablas listos")


def loaded_batches(api):
    rows = api.execute(f"SELECT batch_id, source, entity FROM {RAW_SCHEMA}.batch_control WHERE status = 'loaded'")
    return {(batch_id, source, entity) for batch_id, source, entity in rows}


def load_pending(api, manifests=None, settings=None):
    """Carga en orden los lotes confirmados en S3 que no figuran como cargados. Devuelve los manifiestos cargados."""
    settings = settings or api.settings
    manifests = list_batches(settings["bucket"]) if manifests is None else manifests
    done = loaded_batches(api)
    pending = sorted(
        (m for m in manifests if (m["batch_id"], m["source"], m["entity"]) not in done),
        key=lambda m: (m["batch_id"], m["source"], m["entity"]),
    )
    for manifest in pending:
        api.transaction(batch_statements(manifest, settings["bucket"], settings["role_arn"], settings["region"]))
        LOGGER.info("Cargado lote %s %s.%s (%s filas)", manifest["batch_id"], manifest["source"], manifest["entity"], manifest["rows"])
    mismatches = api.execute(f"SELECT batch_id, entity, rows_expected, rows_loaded FROM {RAW_SCHEMA}.batch_control WHERE status <> 'loaded'")
    if mismatches:
        raise RuntimeError(f"Lotes con conteos distintos entre S3 y Redshift: {mismatches}")
    return pending


def status(api):
    print("\nraw.batch_control")
    print(f"{'lote':<17}{'fuente':<9}{'entidad':<19}{'tipo':<14}{'esperadas':>10}{'cargadas':>10}  estado")
    for r in api.execute(f"SELECT batch_id, source, entity, batch_type, rows_expected, rows_loaded, status FROM {RAW_SCHEMA}.batch_control ORDER BY batch_id, source, entity"):
        print(f"{r[0]:<17}{r[1]:<9}{r[2]:<19}{r[3]:<14}{r[4]:>10}{r[5]:>10}  {r[6]}")
    print("\nFilas actuales en raw")
    for source, entity, _ in definitions():
        count = api.execute(f"SELECT COUNT(*) FROM {raw_table(source, entity)}")[0][0]
        print(f"  {raw_table(source, entity):<32}{count:>8}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--init", action="store_true")
    parser.add_argument("--load", action="store_true")
    parser.add_argument("--status", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    api = DataApi(settings=redshift_settings())
    if args.init:
        init_schema(api)
    if args.load:
        loaded = load_pending(api)
        print(f"{len(loaded)} lote(s)/entidad(es) cargados")
    if args.status or not (args.init or args.load):
        status(api)


if __name__ == "__main__":
    main()
