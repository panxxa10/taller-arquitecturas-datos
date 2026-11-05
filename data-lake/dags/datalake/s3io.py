import json
from functools import lru_cache

import boto3
from botocore.exceptions import ClientError


@lru_cache(maxsize=None)
def client(region: str):
    return boto3.client("s3", region_name=region)


def put_bytes(cfg: dict, key: str, data: bytes, content_type: str = "application/octet-stream") -> str:
    client(cfg["region"]).put_object(Bucket=cfg["bucket"], Key=key, Body=data, ContentType=content_type)
    return f"s3://{cfg['bucket']}/{key}"


def put_json(cfg: dict, key: str, obj) -> str:
    body = json.dumps(obj, ensure_ascii=False, indent=2, default=str).encode("utf-8")
    return put_bytes(cfg, key, body, "application/json")


def get_bytes(cfg: dict, key: str) -> bytes:
    return client(cfg["region"]).get_object(Bucket=cfg["bucket"], Key=key)["Body"].read()


def get_json(cfg: dict, key: str):
    return json.loads(get_bytes(cfg, key))


def exists(cfg: dict, key: str) -> bool:
    try:
        client(cfg["region"]).head_object(Bucket=cfg["bucket"], Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def list_keys(cfg: dict, prefix: str) -> list[str]:
    paginator = client(cfg["region"]).get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=cfg["bucket"], Prefix=prefix):
        keys.extend(obj["Key"] for obj in page.get("Contents", []))
    return keys


def delete_prefix(cfg: dict, prefix: str) -> int:
    keys = list_keys(cfg, prefix)
    s3 = client(cfg["region"])
    for i in range(0, len(keys), 1000):
        s3.delete_objects(Bucket=cfg["bucket"], Delete={"Objects": [{"Key": k} for k in keys[i:i + 1000]]})
    return len(keys)


FAIL_PREFIX = "control/fail_injection/"


def check_fail_injection(cfg: dict, stage: str):
    if exists(cfg, f"{FAIL_PREFIX}{stage}"):
        raise RuntimeError(f"Fallo controlado activo en la etapa '{stage}' ({FAIL_PREFIX}{stage})")
