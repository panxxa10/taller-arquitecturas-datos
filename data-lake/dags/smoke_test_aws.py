from datetime import datetime

from airflow.sdk import dag, task


@dag(dag_id="smoke_test_aws", schedule=None, start_date=datetime(2026, 1, 1), catchup=False, tags=["setup"])
def smoke_test_aws():
    @task
    def check_s3():
        import boto3
        from datalake.config import load_pipeline

        cfg = load_pipeline()
        s3 = boto3.client("s3", region_name=cfg["region"])
        key = "control/smoke_test.txt"
        s3.put_object(Bucket=cfg["bucket"], Key=key, Body=b"ok")
        return f"s3://{cfg['bucket']}/{key}"

    @task
    def check_comprehend():
        import boto3
        from datalake.config import load_pipeline

        cfg = load_pipeline()
        client = boto3.client("comprehend", region_name=cfg["region"])
        text = "El Banco de la República anunció en Bogotá una reunión con Juan Pérez."
        resp = client.detect_entities(Text=text, LanguageCode="es")
        return [(e["Type"], e["Text"], round(e["Score"], 3)) for e in resp["Entities"]]

    @task
    def check_pyarrow():
        import pyarrow

        return pyarrow.__version__

    @task
    def check_rss():
        import requests
        from datalake.config import load_sources

        result = {}
        for src in load_sources():
            r = requests.get(src["url"], timeout=20, headers={"User-Agent": "datalake-noticias-eia/1.0"})
            result[src["id"]] = {"status": r.status_code, "items": r.text.count("<item")}
        return result

    [check_s3(), check_comprehend(), check_pyarrow(), check_rss()]


smoke_test_aws()
