"""Pruebas de la extracción por lotes con S3 simulado (moto) y fuentes simuladas."""

import gzip
import json
import sys
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from ecommerce_dw.ingestion import s3_extract as ex  # noqa: E402

BUCKET = "test-bucket"
SOURCE_DATA = {}  # (fuente, entidad) -> lista de filas; la llave es la primera columna


def fake_fetch(source, name, columns, key, watermark):
    rows = SOURCE_DATA[(source, name)]
    for row in rows:
        if watermark is None or row[0] > watermark:
            yield row


@pytest.fixture
def s3(monkeypatch):
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        monkeypatch.setattr(ex, "fetch_rows", fake_fetch)
        SOURCE_DATA.clear()
        for source, name, columns, *_ in ex.entities():
            SOURCE_DATA[(source, name)] = []
        SOURCE_DATA[("mysql", "orders")] = [(i, 1, "2026-01-01 10:00:00", "pagado", "web", "PSE", 10.5) for i in (1, 2, 3)]
        SOURCE_DATA[("mysql", "customers")] = [(1, "Ana", "a@x.co", "1", "Bogotá", "Bogotá, D.C.", "2025-01-01", "2025-01-01 00:00:00")]
        yield client


def rows_of(s3, key):
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    return gzip.decompress(body).decode().strip().splitlines()


def run(s3, batch_id, only=None):
    return {(m["entity"]): m for m in ex.extract_batch(batch_id, only, bucket=BUCKET, s3=s3)}


def test_initial_load_writes_data_manifest_and_metadata(s3):
    result = run(s3, "20261103T100000")
    orders = result["orders"]
    assert orders["batch_type"] == "initial" and orders["rows"] == 3 and orders["watermark_to"] == 3
    key = "raw/mysql/orders/batch_id=20261103T100000/part-0000.csv.gz"
    lines = rows_of(s3, key)
    assert lines[0].endswith("_batch_id,_extracted_at") and len(lines) == 4
    assert "20261103T100000" in lines[1]
    manifest = json.loads(s3.get_object(Bucket=BUCKET, Key=key.replace("part-0000.csv.gz", "_manifest.json"))["Body"].read())
    assert manifest["files"][0]["sha256"] and manifest["status"] == "committed"


def test_repeating_a_batch_does_not_query_sources_again(s3, monkeypatch):
    run(s3, "20261103T100000")
    monkeypatch.setattr(ex, "fetch_rows", lambda *a: (_ for _ in ()).throw(AssertionError("consultó la fuente")))
    again = run(s3, "20261103T100000")
    assert again["orders"]["reused"] and again["orders"]["rows"] == 3


def test_incremental_batch_only_takes_new_rows_and_snapshots_dimensions(s3):
    run(s3, "20261103T100000")
    SOURCE_DATA[("mysql", "orders")] += [(4, 1, "2026-01-02 10:00:00", "pagado", "web", "PSE", 5.0)]
    SOURCE_DATA[("mysql", "customers")] = [(1, "Ana", "a@x.co", "1", "Medellín", "Antioquia", "2025-01-01", "2025-07-01 00:00:00")]
    result = run(s3, "20261104T100000")
    assert result["orders"]["batch_type"] == "incremental" and result["orders"]["rows"] == 1
    assert result["orders"]["watermark_from"] == 3 and result["orders"]["watermark_to"] == 4
    # la dimensión se reextrae completa: capta el cambio aunque su fecha sea anterior a la marca de agua
    assert result["customers"]["batch_type"] == "full_snapshot" and result["customers"]["rows"] == 1


def test_empty_incremental_batch_is_recorded_without_files(s3):
    run(s3, "20261103T100000")
    result = run(s3, "20261104T100000")
    assert result["orders"]["rows"] == 0 and result["orders"]["files"] == []
    assert result["orders"]["watermark_to"] == 3  # la marca de agua no retrocede


def test_failed_attempt_leaves_no_manifest_and_can_be_retried(s3, monkeypatch):
    def broken(*args):
        yield (1, 1, "2026-01-01 10:00:00", "pagado", "web", "PSE", 1.0)
        raise RuntimeError("fallo de red")

    monkeypatch.setattr(ex, "fetch_rows", broken)
    with pytest.raises(RuntimeError):
        run(s3, "20261103T100000", only={"orders"})
    assert ex.committed_batches(s3, BUCKET, "mysql", "orders") == []
    monkeypatch.setattr(ex, "fetch_rows", fake_fetch)
    ok = run(s3, "20261103T100000", only={"orders"})
    assert ok["orders"]["rows"] == 3 and not ok["orders"]["reused"]


def test_older_batch_id_is_rejected(s3):
    run(s3, "20261104T100000")
    with pytest.raises(ex.BatchError):
        run(s3, "20261103T100000")


def test_batch_id_validation_and_airflow_format():
    assert ex.normalize_batch_id("20261103T100000+0000") == "20261103T100000"
    with pytest.raises(ex.BatchError):
        ex.normalize_batch_id("lote-1")
