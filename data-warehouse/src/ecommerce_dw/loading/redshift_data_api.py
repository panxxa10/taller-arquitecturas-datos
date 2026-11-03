"""Cliente mínimo de la API de datos de Redshift: enviar SQL, esperar a que termine y leer resultados."""

import time

import boto3

from ecommerce_dw.aws_config import redshift_settings

TERMINAL = {"FINISHED", "FAILED", "ABORTED"}


class RedshiftError(Exception):
    pass


class DataApi:
    def __init__(self, client=None, settings=None, poll_seconds=1.0, timeout_seconds=600):
        self.settings = settings or redshift_settings()
        self.client = client or boto3.client("redshift-data", region_name=self.settings["region"])
        self.poll_seconds = poll_seconds
        self.timeout_seconds = timeout_seconds

    def _target(self):
        return {
            "WorkgroupName": self.settings["workgroup"],
            "Database": self.settings["database"],
            "SecretArn": self.settings["secret_arn"],
        }

    def _wait(self, statement_id):
        deadline = time.monotonic() + self.timeout_seconds
        while True:
            description = self.client.describe_statement(Id=statement_id)
            status = description["Status"]
            if status in TERMINAL:
                if status != "FINISHED":
                    raise RedshiftError(f"Sentencia {statement_id} {status}: {description.get('Error', 'sin detalle')}")
                return description
            if time.monotonic() > deadline:
                raise RedshiftError(f"Sentencia {statement_id} superó {self.timeout_seconds}s")
            time.sleep(self.poll_seconds)

    def execute(self, sql):
        """Ejecuta una sentencia y devuelve sus filas (lista de listas; None para NULL)."""
        statement_id = self.client.execute_statement(Sql=sql, **self._target())["Id"]
        description = self._wait(statement_id)
        if not description.get("HasResultSet"):
            return []
        rows, token = [], None
        while True:
            kwargs = {"Id": statement_id, **({"NextToken": token} if token else {})}
            page = self.client.get_statement_result(**kwargs)
            for record in page["Records"]:
                rows.append([None if field.get("isNull") else next(iter(field.values())) for field in record])
            token = page.get("NextToken")
            if not token:
                return rows

    def transaction(self, statements):
        """Ejecuta varias sentencias como UNA transacción: todas se aplican o ninguna."""
        statement_id = self.client.batch_execute_statement(Sqls=list(statements), **self._target())["Id"]
        return self._wait(statement_id)
