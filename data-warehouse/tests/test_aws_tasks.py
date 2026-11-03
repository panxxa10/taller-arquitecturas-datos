import pytest

from ecommerce_dw.orchestration import aws_tasks as t


class FakeApi:
    def __init__(self, rows=None):
        self.sql, self.rows = [], rows or []

    def execute(self, sql):
        self.sql.append(sql)
        return self.rows if sql.lstrip().upper().startswith("SELECT") else []


def test_batch_id_comes_from_parameter_or_logical_date():
    assert t.resolve_batch_id("", "20261103T143000") == "20261103T143000"
    assert t.resolve_batch_id("  20260101T000000 ", "20261103T143000") == "20260101T000000"
    assert t.resolve_batch_id(None, "20261103T143000+0000") == "20261103T143000"
    with pytest.raises(Exception):
        t.resolve_batch_id("lote-malo", "x")


def test_controlled_failure_only_hits_requested_stage():
    t.maybe_fail("load", "none", "r", "b")
    t.maybe_fail("load", "extract", "r", "b")
    with pytest.raises(RuntimeError, match="FALLO CONTROLADO"):
        t.maybe_fail("load", "load", "r", "b")
    with pytest.raises(ValueError):
        t.maybe_fail("load", "otro", "r", "b")


def test_record_result_logs_success_and_failure(monkeypatch):
    api = FakeApi()
    monkeypatch.setattr(t, "DataApi", lambda: api)
    states = {"extract_to_s3": "success", "dbt_test": "success"}
    assert t.record_result("run1", "20261103T143000", "dw_pipeline", "2026-11-03T14:30:00+00:00", states) == "success"
    insert = api.sql[-1]
    assert insert.startswith('INSERT INTO "raw".pipeline_run_log') and "'success'" in insert and "'run1'" in insert

    api.sql.clear()
    states = {"extract_to_s3": "success", "load_redshift": "failed", "dbt_deps": "upstream_failed"}
    with pytest.raises(RuntimeError, match="fallos"):
        t.record_result("run2", "20261103T143000", "dw_pipeline", "2026-11-03T14:30:00+00:00", states)
    assert "'failed'" in api.sql[-1] and "dbt_deps,load_redshift" in api.sql[-1]


def test_load_fails_without_batch_in_control_table(monkeypatch):
    api = FakeApi(rows=[])
    monkeypatch.setattr(t, "DataApi", lambda: api)
    monkeypatch.setattr(t, "init_schema", lambda a: None)
    monkeypatch.setattr(t, "load_pending", lambda a: [])
    with pytest.raises(RuntimeError, match="batch_control"):
        t.load_to_redshift("r", "20261103T143000")


def test_validate_reports_missing_parameters(monkeypatch):
    for name in t.REQUIRED_ENV:
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(RuntimeError, match="parámetros incompletos"):
        t.validate_connections("r", "20261103T143000")


def test_evidence_report_reconciles_with_fake_warehouse(monkeypatch, capsys):
    from ecommerce_dw.orchestration import evidence

    class Api:
        def execute(self, sql):
            if "UNION ALL" in sql or "pipeline_run_log" in sql or "GROUP BY batch_id" in sql:
                return []
            if "SUM(subtotal)" in sql:
                return [["100.00"]] if "quarantine" not in sql else [["10.00"]]
            if "SUM(subtotal)" not in sql and "COUNT" in sql:
                return [[5]]
            return [[0]]

    monkeypatch.setattr(evidence, "mysql_counts", lambda: {"mysql_orders": 5})
    # válidas 5 + cuarentena 5 != raw 5 -> debe señalar que no cuadra
    assert evidence.report(Api()) is False
    assert "NO CUADRA" in capsys.readouterr().out


def test_quicksight_user_is_read_only_on_marts():
    from ecommerce_dw.loading import quicksight_user as q

    password = q.new_password()
    assert 8 <= len(password) <= 64 and any(c.isupper() for c in password) and any(c.isdigit() for c in password)
    created = q.statements(False, password, "dwadmin")
    assert created[0].startswith("CREATE USER quicksight_ro") and q.statements(True, password, "dwadmin")[0].startswith("ALTER USER")
    assert all("marts" in s or "USER" in s for s in created)
    assert not any("INSERT" in s or "ALL PRIVILEGES" in s or "public" in s for s in created)


def test_indicator_report_matches_fact_total(capsys):
    from ecommerce_dw.orchestration import indicator

    class Api:
        def execute(self, sql):
            if sql == indicator.TOTAL_SQL:
                return [["100.50", 3]]
            if sql == indicator.FACT_SQL:
                return [["100.50"]]
            return [["Hogar", "60.25"]]

    indicator.main(Api())
    assert "COINCIDE" in capsys.readouterr().out
