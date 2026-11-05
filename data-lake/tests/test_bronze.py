import boto3
import pytest
from moto import mock_aws

from datalake import bronze, s3io

CFG = {"bucket": "test-bucket", "region": "us-east-1", "http": {"timeout_seconds": 5, "user_agent": "test"}}
SOURCE = {"id": "eltiempo_politica", "medio": "El Tiempo", "seccion": "politica", "url": "https://x/rss.xml"}
RSS_OK = b"""<?xml version="1.0"?><rss><channel><title>t</title>
<item><title>A</title><link>https://x/a</link></item>
<item><title>B</title><link>https://x/b</link></item></channel></rss>"""


@pytest.fixture
def s3():
    with mock_aws():
        s3io.client.cache_clear()
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="test-bucket")
        yield


def fake(status, body=b"", error=None):
    calls = []

    def _f(url, timeout, ua):
        calls.append(url)
        return status, body, error

    _f.calls = calls
    return _f


def test_batch_id_deterministic():
    rid = "manual__2026-11-03T20:15:42.123456+00:00"
    assert bronze.make_batch_id(rid) == bronze.make_batch_id(rid)
    assert bronze.make_batch_id(rid).startswith("20261103T201542_")
    assert bronze.batch_date(bronze.make_batch_id(rid)) == "2026-11-03"


@pytest.mark.parametrize("body,status", [
    (b"", bronze.EMPTY),
    (b"<rss><channel><item>", bronze.MALFORMED),
    (b"<rss><channel></channel></rss>", bronze.NO_ITEMS),
    (RSS_OK, bronze.OK),
])
def test_validate_feed(body, status):
    assert bronze.validate_feed(body)[0] == status


def test_ingest_ok_writes_raw_and_manifest(s3):
    m = bronze.ingest_source(CFG, SOURCE, "20261103T201542_abcdef", fetcher=fake(200, RSS_OK))
    assert m["status"] == bronze.OK and m["item_count"] == 2
    prefix = "bronze/source=eltiempo_politica/ingestion_date=2026-11-03/run_id=20261103T201542_abcdef/"
    assert s3io.get_bytes(CFG, prefix + "feed.xml") == RSS_OK
    assert s3io.exists(CFG, prefix + "manifest.json")


def test_retry_same_batch_does_not_refetch(s3):
    bronze.ingest_source(CFG, SOURCE, "20261103T201542_abcdef", fetcher=fake(200, RSS_OK))
    f = fake(200, b"otro contenido")
    m = bronze.ingest_source(CFG, SOURCE, "20261103T201542_abcdef", fetcher=f)
    assert f.calls == [] and m["item_count"] == 2


def test_http_error_not_stored_as_raw(s3):
    m = bronze.ingest_source(CFG, SOURCE, "20261103T201542_abcdef", fetcher=fake(503, b"down"))
    assert m["status"] == bronze.HTTP_ERROR and m["http_status"] == 503
    assert not any(k.endswith("feed.xml") for k in s3io.list_keys(CFG, "bronze/"))
    assert s3io.list_keys(CFG, "quarantine/bronze/")


def test_network_error(s3):
    m = bronze.ingest_source(CFG, SOURCE, "20261103T201542_abcdef", fetcher=fake(None, b"", "Timeout"))
    assert m["status"] == bronze.NETWORK_ERROR


def test_batch_outcome():
    assert bronze.batch_outcome([{"status": "ok"}] * 3, 3) == "complete"
    assert bronze.batch_outcome([{"status": "ok"}, {"status": "http_error"}], 2) == "partial"
    assert bronze.batch_outcome([{"status": "http_error"}], 1) == "failed"
