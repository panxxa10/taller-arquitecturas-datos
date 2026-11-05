from datetime import datetime

import boto3
import pytest
from moto import mock_aws

from datalake import bronze, s3io, silver

CFG = {"bucket": "test-bucket", "region": "us-east-1", "http": {"timeout_seconds": 5, "user_agent": "t"},
       "comprehend": {"language": "es", "max_text_bytes": 4500, "min_text_chars": 5}}


class FakeComprehend:
    def __init__(self):
        self.calls = 0

    def detect_dominant_language(self, Text):
        return {"Languages": [{"LanguageCode": "es", "Score": 0.99}]}

    def detect_entities(self, Text, LanguageCode):
        self.calls += 1
        return {"Entities": [{"Type": "ORGANIZATION", "Text": "Noticia", "Score": 0.9}]}

    def detect_key_phrases(self, Text, LanguageCode):
        return {"KeyPhrases": [{"Text": "la noticia", "Score": 0.95}]}


def item(title, link, desc="Descripción.", pub="2026-11-03T20:34:03-05:00", cat="Gobierno"):
    pub_xml = f"<pubDate>{pub}</pubDate>" if pub is not None else ""
    return (f"<item><title>{title}</title>{pub_xml}<link>{link}</link><guid>{link}</guid>"
            f"<category>{cat}</category><description>{desc}</description></item>")


def feed(*items):
    return f'<?xml version="1.0"?><rss xmlns:slash="http://purl.org/rss/1.0/modules/slash/"><channel>{"".join(items)}</channel></rss>'.encode()


A = "https://www.eltiempo.com/politica/gobierno/noticia-a-3598267"
B = "https://www.eltiempo.com/justicia/delitos/noticia-b-3598300"


@pytest.fixture
def s3():
    with mock_aws():
        s3io.client.cache_clear()
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="test-bucket")
        yield


def ingest(run_id, feeds: dict):
    for seccion, content in feeds.items():
        src = {"id": f"eltiempo_{seccion}", "medio": "El Tiempo", "seccion": seccion, "url": f"https://x/{seccion}.xml"}
        bronze.ingest_source(CFG, src, run_id, fetcher=lambda *a, c=content: (200, c, None))


def current_news():
    return silver.read_state(CFG)


def test_news_id_uses_article_number():
    assert silver.make_news_id(A + "/", None) == "eltiempo.com-3598267"
    assert silver.make_news_id(A + "?utm=x", None) == "eltiempo.com-3598267"
    assert silver.make_news_id("https://otro.com/sin-numero", None).startswith("h-")


def test_clean_text_removes_html_and_entities():
    assert silver.clean_text("<p>Hola &amp;lt;b&amp;gt; <b>mundo</b></p>\n  ok") == "Hola mundo ok"


@pytest.mark.parametrize("raw,status", [
    ("2026-11-03T20:34:03-05:00", "ok"),
    ("Tue, 03 Nov 2026 20:34:03 -0500", "ok"),
    ("", "missing"),
    (None, "missing"),
    ("ayer en la tarde", "invalid"),
    ("2031-01-01T00:00:00Z", "invalid"),
])
def test_parse_pub_date(raw, status):
    ts, st = silver.parse_pub_date(raw, datetime.fromisoformat("2026-11-04T04:00:00+00:00"))
    assert st == status
    if status == "ok":
        assert ts == datetime(2026, 11, 4, 1, 34, 3)


def test_initial_load_dedup_and_quarantine(s3):
    ingest("20261104T040000_aaaaaa", {
        "politica": feed(item("Noticia A", A), item("<b>Noticia B</b>", B, desc="<p>Con HTML</p>")),
        "justicia": feed(item("Noticia B", B), item("", "https://www.eltiempo.com/x-3599999"), item("Sin enlace", "notaurl")),
    })
    report = silver.build_silver(CFG, ["20261104T040000_aaaaaa"], from_scratch=False, attempt="t1", comprehend=FakeComprehend())
    run = report["runs"]["20261104T040000_aaaaaa"]
    assert run["received"] == 5 and run["rejected"] == 2 and run["unique_in_batch"] == 2
    assert run["duplicates_in_batch"] == 1 and run["multi_feed_news"] == 1 and run["new"] == 2
    news = current_news()
    b = news["eltiempo.com-3598300"]
    assert b["secciones"] == ["justicia", "politica"] and b["title"] == "Noticia B"
    rejected = s3io.get_bytes(CFG, "quarantine/silver/run_id=20261104T040000_aaaaaa/rejected.jsonl").decode()
    assert "titulo_vacio" in rejected and "enlace_invalido" in rejected and "bronze_uri" in rejected


def test_reprocess_same_batch_is_idempotent(s3):
    ingest("20261104T040000_aaaaaa", {"politica": feed(item("Noticia A", A), item("Noticia B", B))})
    silver.build_silver(CFG, ["20261104T040000_aaaaaa"], False, "t1", comprehend=FakeComprehend())
    first = current_news()
    report = silver.build_silver(CFG, ["20261104T040000_aaaaaa"], False, "t2", comprehend=FakeComprehend())
    assert report["runs"]["20261104T040000_aaaaaa"]["repeated"] == 2
    assert report["runs"]["20261104T040000_aaaaaa"]["new"] == 0
    assert current_news() == first


def test_incremental_and_modified_news(s3):
    ingest("20261104T040000_aaaaaa", {"politica": feed(item("Noticia A", A))})
    silver.build_silver(CFG, ["20261104T040000_aaaaaa"], False, "t1", comprehend=FakeComprehend())
    ingest("20261104T060000_bbbbbb", {"politica": feed(item("Noticia A corregida", A), item("Noticia B", B))})
    report = silver.build_silver(CFG, ["20261104T060000_bbbbbb"], False, "t2", comprehend=FakeComprehend())
    run = report["runs"]["20261104T060000_bbbbbb"]
    assert run["new"] == 1 and run["updated"] == 1
    a = current_news()["eltiempo.com-3598267"]
    assert a["title"] == "Noticia A corregida" and a["version"] == 2
    assert a["first_seen_run_id"] == "20261104T040000_aaaaaa" and a["enrichment_status"] == "enriched"

    stale = silver.build_silver(CFG, ["20261104T040000_aaaaaa"], False, "t3", comprehend=FakeComprehend())
    assert stale["runs"]["20261104T040000_aaaaaa"]["stale"] == 1
    assert current_news()["eltiempo.com-3598267"]["title"] == "Noticia A corregida"


def test_rebuild_from_bronze_matches_incremental(s3):
    ingest("20261104T040000_aaaaaa", {"politica": feed(item("Noticia A", A))})
    silver.build_silver(CFG, ["20261104T040000_aaaaaa"], False, "t1", comprehend=FakeComprehend())
    ingest("20261104T060000_bbbbbb", {"politica": feed(item("Noticia A v2", A), item("Noticia B", B, pub=None))})
    silver.build_silver(CFG, ["20261104T060000_bbbbbb"], False, "t2", comprehend=FakeComprehend())
    incremental = current_news()
    silver.build_silver(CFG, silver.list_bronze_run_ids(CFG), True, "t3", comprehend=FakeComprehend())
    assert current_news() == incremental
    keys = s3io.list_keys(CFG, "silver/news/")
    assert any("pub_date=unknown" in k for k in keys) and any("pub_date=2026-11-04" in k for k in keys)


def test_old_versions_cleaned_and_pointer_kept(s3):
    ingest("20261104T040000_aaaaaa", {"politica": feed(item("Noticia A", A))})
    for i in range(5):
        silver.build_silver(CFG, ["20261104T040000_aaaaaa"], False, f"t{i}", comprehend=FakeComprehend())
    versions = {k.split("version=")[1].split("/")[0] for k in s3io.list_keys(CFG, "silver/news/")}
    assert len(versions) == 3
    assert silver.read_pointer(CFG)["version"] in versions
