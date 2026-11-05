import boto3
import pytest
from moto import mock_aws

from datalake import catalog, gold, s3io, silver
from test_silver import CFG, A, B, feed, ingest, item
from test_enrich import SpanishOrEnglish

CFG_GLUE = {**CFG, "glue_database": "noticias_dl"}


@pytest.fixture
def aws():
    with mock_aws():
        s3io.client.cache_clear()
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="test-bucket")
        yield


def load():
    ingest("20261104T040000_aaaaaa", {
        "politica": feed(item("Noticia A", A), item("Noticia B", B)),
        "justicia": feed(item("Noticia B", B), item("Sin fecha", "https://www.eltiempo.com/x-3597777", pub=None)),
    })
    silver.build_silver(CFG, ["20261104T040000_aaaaaa"], False, "t1", comprehend=SpanishOrEnglish())
    return gold.build_gold(CFG)


def test_aggregates_count_distinct_news(aws):
    report = load()
    assert report["news_without_valid_date"] == 1
    pointer = s3io.get_json(CFG, gold.POINTER_KEY)
    rows = {}
    for table in gold.SCHEMAS:
        rows[table] = []
        for d, loc in pointer["partitions"][table].items():
            key = loc.split("s3://test-bucket/")[1] + "part-0.parquet"
            rows[table] += [{**r, "pub_date": d} for r in silver.read_parquet_bytes(s3io.get_bytes(CFG, key))]

    daily = {(r["pub_date"], r["seccion"]): r["news_count"] for r in rows["news_daily"]}
    assert daily == {("2026-11-04", "politica"): 2, ("2026-11-04", "justicia"): 1, ("unknown", "justicia"): 1}

    person = [r for r in rows["entity_daily"] if r["entity_type"] == "PERSON" and r["pub_date"] == "2026-11-04"][0]
    assert person["news_count"] == 2 and person["mentions"] == 4 and person["entity_text"] == "Juan Pérez"


def test_catalog_sync_creates_and_moves_partitions(aws):
    load()
    glue = boto3.client("glue", region_name="us-east-1")
    r1 = catalog.sync_catalog(CFG_GLUE, gold.published_tables(CFG_GLUE), glue)
    assert set(r1) == {"silver_news", "silver_news_entities", "silver_news_key_phrases",
                       "gold_news_daily", "gold_entity_daily", "gold_key_phrase_section_daily"}
    assert r1["silver_news"]["created"] == 2

    table = glue.get_table(DatabaseName="noticias_dl", Name="silver_news")["Table"]
    cols = {c["Name"]: c["Type"] for c in table["StorageDescriptor"]["Columns"]}
    assert cols["secciones"] == "array<string>" and cols["pub_ts_utc"] == "timestamp"
    assert table["PartitionKeys"][0]["Name"] == "pub_date"

    silver.build_silver(CFG, ["20261104T040000_aaaaaa"], False, "t2", comprehend=SpanishOrEnglish())
    gold.build_gold(CFG)
    r2 = catalog.sync_catalog(CFG_GLUE, gold.published_tables(CFG_GLUE), glue)
    assert r2["silver_news"] == {"created": 0, "updated": 2, "deleted": 0, "total": 2}
    parts = catalog.existing_partitions(glue, "noticias_dl", "silver_news")
    assert all("__t2/" in loc for loc in parts.values())
