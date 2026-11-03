"""Pruebas de la carga a Redshift con una API de datos simulada."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ecommerce_dw.loading import ddl, redshift_loader  # noqa: E402

SETTINGS = {"bucket": "bkt", "role_arn": "arn:aws:iam::1:role/r", "region": "us-east-1"}


def manifest(entity="orders", source="mysql", mode="incremental", rows=3, batch="20261103T100000", btype="initial"):
    return {
        "batch_id": batch, "source": source, "entity": entity, "batch_type": btype, "mode": mode, "key": "order_id",
        "extracted_at": "2026-11-03T10:00:00Z", "rows": rows, "run_id": None,
        "files": [{"key": f"raw/{source}/{entity}/batch_id={batch}/part-0000.csv.gz"}] if rows else [],
    }


class FakeApi:
    settings = SETTINGS

    def __init__(self, loaded=()):
        self.loaded, self.transactions, self.queries = list(loaded), [], []

    def execute(self, sql):
        self.queries.append(sql)
        if "status = 'loaded'" in sql:
            return self.loaded
        return []

    def transaction(self, statements):
        self.transactions.append(statements)


def test_raw_schema_is_always_quoted_because_raw_is_reserved():
    statements = ddl.create_statements()
    assert statements[0] == 'CREATE SCHEMA IF NOT EXISTS "raw"'
    assert all('"raw".' in s for s in statements[1:])
    assert any('"raw".mongo_reviews' in s and "document_json VARCHAR(65535)" in s for s in statements)
    assert len(statements) == 2 + 6  # esquema + control + 6 tablas


def test_incremental_batch_copies_replaces_by_key_and_registers_control():
    sqls = ddl.batch_statements(manifest(), "bkt", SETTINGS["role_arn"], "us-east-1")
    kinds = [s.split()[0] + " " + s.split()[1] for s in sqls]
    assert kinds == ["CREATE TEMP", "COPY stg_mysql_orders", "DELETE FROM", "INSERT INTO", "DELETE FROM", "INSERT INTO"]
    assert "GZIP IGNOREHEADER 1" in sqls[1] and "s3://bkt/raw/mysql/orders/batch_id=20261103T100000/part-0000.csv.gz" in sqls[1]
    assert 'WHERE order_id IN (SELECT order_id FROM stg_mysql_orders)' in sqls[2]
    assert "'count_mismatch'" in sqls[-1] and "'loaded'" in sqls[-1]


def test_full_snapshot_replaces_whole_table():
    m = manifest("customers", mode="full", btype="full_snapshot")
    m["key"] = "customer_id"
    sqls = ddl.batch_statements(m, "bkt", SETTINGS["role_arn"], "us-east-1")
    assert 'DELETE FROM "raw".mysql_customers' in sqls and not any("IN (SELECT" in s for s in sqls[:4])


def test_empty_incremental_batch_only_registers_control_row():
    sqls = ddl.batch_statements(manifest(rows=0), "bkt", SETTINGS["role_arn"], "us-east-1")
    assert len(sqls) == 2 and "COPY" not in " ".join(sqls) and "0, 0, 'loaded'" in sqls[-1]


def test_only_pending_batches_are_loaded_in_order():
    api = FakeApi(loaded=[["20261103T100000", "mysql", "orders"]])
    manifests = [manifest(batch="20261104T100000"), manifest(batch="20261103T100000"), manifest("customers", batch="20261103T100000")]
    manifests[2]["key"] = "customer_id"
    loaded = redshift_loader.load_pending(api, manifests, SETTINGS)
    assert [(m["batch_id"], m["entity"]) for m in loaded] == [("20261103T100000", "customers"), ("20261104T100000", "orders")]
    assert len(api.transactions) == 2


def test_repeating_load_with_everything_registered_does_nothing():
    api = FakeApi(loaded=[["20261103T100000", "mysql", "orders"]])
    assert redshift_loader.load_pending(api, [manifest()], SETTINGS) == [] and api.transactions == []


def test_quotes_in_values_are_escaped():
    assert ddl.lit("it's") == "'it''s'"
