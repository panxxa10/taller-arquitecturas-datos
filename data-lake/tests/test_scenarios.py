import boto3
import pytest
from moto import mock_aws

from datalake import s3io, scenarios, silver
from test_silver import CFG, A, B, feed, ingest, item, FakeComprehend

SOURCES = [{"id": f"eltiempo_{s}", "medio": "El Tiempo", "seccion": s, "url": f"https://x/{s}.xml"}
           for s in ("justicia", "politica")]
C = "https://www.eltiempo.com/justicia/x-3590001"
D = "https://www.eltiempo.com/justicia/x-3590002"


@pytest.fixture
def env(monkeypatch):
    with mock_aws():
        s3io.client.cache_clear()
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="test-bucket")
        monkeypatch.setattr(scenarios, "load_pipeline", lambda: CFG)
        monkeypatch.setattr(scenarios, "load_sources", lambda: SOURCES)
        ingest("20261104T040000_aaaaaa", {"justicia": feed(item("A", A), item("B", B), item("C", C), item("D", D)),
                                          "politica": feed(item("A", A))})
        silver.build_silver(CFG, ["20261104T040000_aaaaaa"], False, "t1", comprehend=FakeComprehend())
        yield


def test_defective_batch(env):
    run_id = scenarios.create_controlled_batch("defectuoso")
    r = silver.build_silver(CFG, [run_id], False, "t2", comprehend=FakeComprehend())["runs"][run_id]
    assert r["rejected"] == 2 and r["duplicates_in_batch"] == 1 and r["new"] == 0
    assert len(silver.read_state(CFG)) == 4


def test_modified_batch_reenriches_one(env):
    run_id = scenarios.create_controlled_batch("modificado")
    c = FakeComprehend()
    r = silver.build_silver(CFG, [run_id], False, "t2", comprehend=c)
    assert r["runs"][run_id]["updated"] == 1 and c.calls == 1
    assert silver.read_state(CFG)["eltiempo.com-3598267"]["version"] == 2


def test_fail_injection_keeps_previous_version(env):
    before = silver.read_pointer(CFG)["version"]
    scenarios.main(["fallo-activar", "silver_before_publish"])
    with pytest.raises(RuntimeError):
        silver.build_silver(CFG, ["20261104T040000_aaaaaa"], False, "t2", comprehend=FakeComprehend())
    assert silver.read_pointer(CFG)["version"] == before
    scenarios.main(["fallo-quitar", "silver_before_publish"])
    silver.build_silver(CFG, ["20261104T040000_aaaaaa"], False, "t3", comprehend=FakeComprehend())
    assert silver.read_pointer(CFG)["version"].endswith("__t3")
