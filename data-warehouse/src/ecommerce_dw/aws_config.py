"""Conexión a las fuentes en AWS: RDS for MySQL y Amazon DocumentDB.

Las contraseñas se leen de AWS Secrets Manager en tiempo de ejecución (secretos
administrados por RDS y DocumentDB). Nunca están en el código ni en archivos .env.
Si no hay variables *_SECRET_ARN se usan MYSQL_USER/MYSQL_PASSWORD/MONGODB_URI del
entorno, lo que mantiene funcionando la versión local con Docker.

Variables:
  AWS_REGION            región (us-east-1)
  MYSQL_HOST            endpoint de RDS
  MYSQL_SECRET_ARN      secreto administrado de RDS ({"username","password"})
  DOCDB_HOST            endpoint del clúster de DocumentDB
  DOCDB_SECRET_ARN      secreto administrado de DocumentDB
  DOCDB_CA_FILE         certificado global-bundle.pem (TLS)
"""

import json
import os
from urllib.parse import quote_plus

_loaded = False
DEFAULT_CA_FILE = "/opt/certs/global-bundle.pem"


def _secret(arn):
    import boto3  # import tardío: la versión local no lo necesita

    client = boto3.client("secretsmanager", region_name=os.getenv("AWS_REGION", "us-east-1"))
    return json.loads(client.get_secret_value(SecretId=arn)["SecretString"])


def ca_file():
    return os.getenv("DOCDB_CA_FILE", DEFAULT_CA_FILE)


def load_source_environment():
    """Completa MYSQL_USER, MYSQL_PASSWORD y MONGODB_URI desde Secrets Manager."""
    global _loaded
    if _loaded:
        return
    mysql_arn = os.getenv("MYSQL_SECRET_ARN")
    if mysql_arn:
        secret = _secret(mysql_arn)
        os.environ["MYSQL_USER"] = secret["username"]
        os.environ["MYSQL_PASSWORD"] = secret["password"]
    docdb_arn, docdb_host = os.getenv("DOCDB_SECRET_ARN"), os.getenv("DOCDB_HOST")
    if docdb_arn and docdb_host:
        secret = _secret(docdb_arn)
        # DocumentDB: TLS obligatorio, sin retryable writes, réplica rs0
        os.environ["MONGODB_URI"] = (
            f"mongodb://{quote_plus(secret['username'])}:{quote_plus(secret['password'])}"
            f"@{docdb_host}:{os.getenv('DOCDB_PORT', '27017')}/"
            f"?authSource=admin&tls=true&tlsCAFile={quote_plus(ca_file())}"
            "&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"
        )
    _loaded = True


def mysql_ssl_options():
    """TLS hacia RDS si MYSQL_SECRET_ARN está definido (modo AWS)."""
    if os.getenv("MYSQL_SECRET_ARN") and os.path.exists(ca_file()):
        return {"ca": ca_file()}
    return None


def get_mongodb_uri():
    load_source_environment()
    return os.environ["MONGODB_URI"]


# ---------------------------------------------------------------- Redshift
def redshift_settings():
    """Parámetros del workgroup (no secretos). La contraseña la usa la Data API vía SecretArn."""
    return {
        "workgroup": os.getenv("REDSHIFT_WORKGROUP", "taller-dw"),
        "database": os.getenv("REDSHIFT_DATABASE", "dw"),
        "secret_arn": os.environ["REDSHIFT_SECRET_ARN"],
        "role_arn": os.environ["REDSHIFT_ROLE_ARN"],
        "region": os.getenv("AWS_REGION", "us-east-1"),
        "bucket": os.environ["S3_BUCKET"],
    }


def load_redshift_environment():
    """Pone REDSHIFT_USER/REDSHIFT_PASSWORD en el entorno del proceso (solo memoria) para dbt."""
    secret = _secret(os.environ["REDSHIFT_SECRET_ARN"])
    os.environ["REDSHIFT_USER"] = secret["username"]
    os.environ["REDSHIFT_PASSWORD"] = secret["password"]
