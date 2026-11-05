import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from datalake import enrich, s3io, silver
from test_silver import CFG, FakeComprehend, A, B, feed, ingest, item


class SpanishOrEnglish(FakeComprehend):
    def detect_dominant_language(self, Text):
        lang = "en" if Text.startswith("Breaking") else "es"
        return {"Languages": [{"LanguageCode": lang, "Score": 0.98}]}

    def detect_entities(self, Text, LanguageCode):
        self.calls += 1
        return {"Entities": [
            {"Type": "PERSON", "Text": "Juan Pérez", "Score": 0.99},
            {"Type": "PERSON", "Text": "juan  pérez", "Score": 0.95},
            {"Type": "LOCATION", "Text": "Bogotá", "Score": 0.97},
            {"Type": "DATE", "Text": "martes", "Score": 0.9},
        ]}


class Broken(FakeComprehend):
    def detect_dominant_language(self, Text):
        raise ClientError({"Error": {"Code": "ThrottlingException", "Message": "x"}}, "DetectDominantLanguage")


@pytest.fixture
def s3():
    with mock_aws():
        s3io.client.cache_clear()
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="test-bucket")
        yield


def tables():
    pointer = silver.read_pointer(CFG)
    out = {}
    for t in silver.TABLES:
        rows = []
        for k in s3io.list_keys(CFG, f"silver/{t}/version={pointer['version']}/"):
            rows.extend(silver.read_parquet_bytes(s3io.get_bytes(CFG, k)))
        out[t] = rows
    return out


def test_entities_are_distinct_and_filtered(s3):
    ingest("20261104T040000_aaaaaa", {"politica": feed(item("Noticia A", A))})
    silver.build_silver(CFG, ["20261104T040000_aaaaaa"], False, "t1", comprehend=SpanishOrEnglish())
    ents = tables()["news_entities"]
    assert {(e["entity_type"], e["entity_norm"]) for e in ents} == {("PERSON", "juan pérez"), ("LOCATION", "bogotá")}
    person = next(e for e in ents if e["entity_type"] == "PERSON")
    assert person["mentions"] == 2 and person["max_score"] == 0.99


def test_cache_avoids_repeated_calls_and_rebuild_costs_nothing(s3):
    ingest("20261104T040000_aaaaaa", {"politica": feed(item("Noticia A", A), item("Noticia B", B))})
    c1 = FakeComprehend()
    silver.build_silver(CFG, ["20261104T040000_aaaaaa"], False, "t1", comprehend=c1)
    assert c1.calls == 2

    c2 = FakeComprehend()
    r = silver.build_silver(CFG, ["20261104T040000_aaaaaa"], False, "t2", comprehend=c2)
    assert c2.calls == 0 and r["enrichment"]["already_enriched"] == 2

    ingest("20261104T060000_bbbbbb", {"politica": feed(item("Noticia A corregida", A), item("Noticia B", B))})
    c3 = FakeComprehend()
    silver.build_silver(CFG, ["20261104T060000_bbbbbb"], False, "t3", comprehend=c3)
    assert c3.calls == 1

    c4 = FakeComprehend()
    r = silver.build_silver(CFG, silver.list_bronze_run_ids(CFG), True, "t4", comprehend=c4)
    assert c4.calls == 0 and r["enrichment"]["cache_hits"] == 2


def test_explicit_statuses(s3):
    long_desc = "palabra " * 700
    ingest("20261104T040000_aaaaaa", {"politica": feed(
        item("Breaking news in English", A),
        item("Corta", B, desc=""),
        item("Muy larga", "https://www.eltiempo.com/x-3591111", desc=long_desc),
    )})
    silver.build_silver(CFG, ["20261104T040000_aaaaaa"], False, "t1", comprehend=SpanishOrEnglish())
    status = {n["news_id"]: n["enrichment_status"] for n in tables()["news"]}
    assert status == {"eltiempo.com-3598267": "unsupported_language", "eltiempo.com-3598300": "enriched",
                      "eltiempo.com-3591111": "skipped_too_long"}


def test_errors_are_marked_and_retried_next_run(s3):
    ingest("20261104T040000_aaaaaa", {"politica": feed(item("Noticia A", A))})
    r = silver.build_silver(CFG, ["20261104T040000_aaaaaa"], False, "t1", comprehend=Broken())
    assert r["enrichment"]["errors"] == 1 and tables()["news"][0]["enrichment_status"] == "error"
    assert tables()["news_entities"] == []
    c = FakeComprehend()
    silver.build_silver(CFG, ["20261104T040000_aaaaaa"], False, "t2", comprehend=c)
    assert c.calls == 1 and tables()["news"][0]["enrichment_status"] == "enriched"


def test_no_orphan_entities_after_update(s3):
    ingest("20261104T040000_aaaaaa", {"politica": feed(item("Noticia A", A))})
    silver.build_silver(CFG, ["20261104T040000_aaaaaa"], False, "t1", comprehend=FakeComprehend())
    ingest("20261104T060000_bbbbbb", {"politica": feed(item("Noticia A corregida", A))})
    silver.build_silver(CFG, ["20261104T060000_bbbbbb"], False, "t2", comprehend=FakeComprehend())
    t = tables()
    news = {n["news_id"]: n["content_hash"] for n in t["news"]}
    for row in t["news_entities"] + t["news_key_phrases"]:
        assert news[row["news_id"]] == row["text_hash"]
