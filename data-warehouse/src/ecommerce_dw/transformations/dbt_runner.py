"""Ejecuta comandos de dbt desde tareas de Airflow o desde la línea de comandos.

Destino local (DuckDB): serializa el acceso al archivo compartido con un lock.
Destino Redshift (DBT_TARGET=redshift): lee usuario y contraseña de Secrets Manager y los pone solo en el
entorno del proceso de dbt; Redshift admite concurrencia, así que no hay lock.

Uso:  python -m ecommerce_dw.transformations.dbt_runner run path:models/intermediate
"""

import fcntl
import logging
import os
import subprocess
import sys
from pathlib import Path

DBT_EXECUTABLE = os.getenv("DBT_EXECUTABLE", "/home/airflow/dbt-venv/bin/dbt")
DBT_PROJECT = Path(os.getenv("DBT_PROJECT_DIR", "/opt/airflow/src/dbt"))
WAREHOUSE_PATH = Path(os.getenv("DUCKDB_PATH", "/opt/warehouse/warehouse.duckdb"))
LOGGER = logging.getLogger(__name__)


def run_dbt(command, selector=None, extra_args=()):
    """Ejecuta dbt y devuelve; lanza CalledProcessError si dbt falla."""
    target = os.getenv("DBT_TARGET", "local")
    arguments = [
        DBT_EXECUTABLE,
        command,
        "--project-dir", str(DBT_PROJECT),
        "--profiles-dir", str(DBT_PROJECT),
        "--target", target,
    ]
    if selector is not None:
        arguments.extend(["--select", selector])
    arguments.extend(extra_args)

    environment = os.environ.copy()
    if target == "redshift":
        from ecommerce_dw.aws_config import load_redshift_environment

        load_redshift_environment()
        environment = os.environ.copy()
    elif command != "deps":
        arguments.extend(["--threads", "1"])

    LOGGER.info("Running dbt command: %s", " ".join(arguments))
    if command == "deps" or target == "redshift":
        subprocess.run(arguments, check=True, env=environment)
        return

    with WAREHOUSE_PATH.with_suffix(".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            subprocess.run(arguments, check=True, env=environment)
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    command, rest = sys.argv[1], sys.argv[2:]
    selector = None
    if rest and not rest[0].startswith("-"):
        selector, rest = rest[0], rest[1:]
    try:
        run_dbt(command, selector, rest)
    except subprocess.CalledProcessError as error:
        sys.exit(error.returncode)
