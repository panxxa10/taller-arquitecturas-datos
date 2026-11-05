import hashlib
import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import requests

from datalake import s3io

log = logging.getLogger(__name__)

OK = "ok"
EMPTY = "empty_response"
HTTP_ERROR = "http_error"
NETWORK_ERROR = "network_error"
MALFORMED = "malformed_xml"
NO_ITEMS = "no_items"


def make_batch_id(run_id: str) -> str:
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})", run_id)
    stamp = "".join(m.groups()[:3]) + "T" + "".join(m.groups()[3:]) if m else datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    return f"{stamp}_{hashlib.sha1(run_id.encode()).hexdigest()[:6]}"


def batch_date(batch_id: str) -> str:
    d = batch_id[:8]
    return f"{d[:4]}-{d[4:6]}-{d[6:8]}"


def bronze_prefix(source_id: str, batch_id: str) -> str:
    return f"bronze/source={source_id}/ingestion_date={batch_date(batch_id)}/run_id={batch_id}/"


def validate_feed(content: bytes) -> tuple[str, int, str | None]:
    if not content or not content.strip():
        return EMPTY, 0, "respuesta vacía"
    try:
        root = ET.fromstring(content)
    except ET.ParseError as e:
        return MALFORMED, 0, f"XML malformado: {e}"
    items = root.findall(".//item")
    if not items:
        return NO_ITEMS, 0, "XML válido sin elementos <item>"
    return OK, len(items), None


def fetch(url: str, timeout: int, user_agent: str) -> tuple[int | None, bytes, str | None]:
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": user_agent})
        return r.status_code, r.content, None
    except requests.RequestException as e:
        return None, b"", f"{type(e).__name__}: {e}"


def ingest_source(cfg: dict, source: dict, batch_id: str, fetcher=fetch) -> dict:
    prefix = bronze_prefix(source["id"], batch_id)
    manifest_key = prefix + "manifest.json"

    if s3io.exists(cfg, manifest_key):
        manifest = s3io.get_json(cfg, manifest_key)
        if manifest["status"] == OK:
            log.info("source=%s batch=%s ya existe en Bronze, no se vuelve a consultar", source["id"], batch_id)
            return manifest

    http_cfg = cfg["http"]
    status_code, content, net_error = fetcher(source["url"], http_cfg["timeout_seconds"], http_cfg["user_agent"])

    if net_error:
        status, item_count, error = NETWORK_ERROR, 0, net_error
    elif status_code != 200:
        status, item_count, error = HTTP_ERROR, 0, f"HTTP {status_code}"
    else:
        status, item_count, error = validate_feed(content)

    content_hash = hashlib.sha256(content).hexdigest() if content else None
    raw_uri = None
    if status == OK:
        raw_uri = s3io.put_bytes(cfg, prefix + "feed.xml", content, "application/rss+xml")
    elif content:
        raw_uri = s3io.put_bytes(cfg, f"quarantine/bronze/source={source['id']}/run_id={batch_id}/response.bin", content)

    manifest = {
        "run_id": batch_id,
        "source_id": source["id"],
        "medio": source["medio"],
        "seccion": source["seccion"],
        "url": source["url"],
        "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
        "http_status": status_code,
        "status": status,
        "error": error,
        "content_sha256": content_hash,
        "content_bytes": len(content),
        "item_count": item_count,
        "raw_s3_uri": raw_uri,
    }
    s3io.put_json(cfg, manifest_key, manifest)
    log.info("source=%s batch=%s status=%s http=%s items=%s", source["id"], batch_id, status, status_code, item_count)
    return manifest


def load_batch_manifests(cfg: dict, batch_id: str) -> list[dict]:
    keys = [k for k in s3io.list_keys(cfg, "bronze/") if f"/run_id={batch_id}/" in k and k.endswith("manifest.json")]
    return [s3io.get_json(cfg, k) for k in keys]


def batch_outcome(manifests: list[dict], expected_sources: int) -> str:
    ok = sum(m["status"] == OK for m in manifests)
    if ok == expected_sources:
        return "complete"
    return "partial" if ok > 0 else "failed"
