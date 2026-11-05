import hashlib
import html
import io
import json
import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit, urlunsplit

import pyarrow as pa
import pyarrow.parquet as pq

from datalake import enrich, s3io
from datalake.bronze import OK, load_batch_manifests

log = logging.getLogger(__name__)

BOGOTA = timezone(timedelta(hours=-5))
TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")
ARTICLE_ID_RE = re.compile(r"-(\d{5,})/?$")

NEWS_PREFIX = "silver/news/"
TABLES = ("news", "news_entities", "news_key_phrases")
POINTER_KEY = "silver/_state/current.json"
UNKNOWN_DATE = "unknown"

NEWS_SCHEMA = pa.schema([
    ("news_id", pa.string()),
    ("guid", pa.string()),
    ("link", pa.string()),
    ("title", pa.string()),
    ("description", pa.string()),
    ("nlp_text", pa.string()),
    ("content_hash", pa.string()),
    ("medio", pa.string()),
    ("seccion", pa.string()),
    ("secciones", pa.list_(pa.string())),
    ("categorias", pa.list_(pa.string())),
    ("pub_ts_utc", pa.timestamp("ms")),
    ("pub_date_raw", pa.string()),
    ("pub_date_status", pa.string()),
    ("first_seen_run_id", pa.string()),
    ("last_seen_run_id", pa.string()),
    ("last_changed_run_id", pa.string()),
    ("version", pa.int32()),
    ("bronze_uri", pa.string()),
    ("updated_at_utc", pa.timestamp("ms")),
    ("enrichment_status", pa.string()),
    ("enriched_text_hash", pa.string()),
    ("enrichment_language", pa.string()),
    ("enriched_at_utc", pa.timestamp("ms")),
])

ENTITY_SCHEMA = pa.schema([
    ("news_id", pa.string()),
    ("entity_type", pa.string()),
    ("entity_text", pa.string()),
    ("entity_norm", pa.string()),
    ("mentions", pa.int32()),
    ("max_score", pa.float64()),
    ("text_hash", pa.string()),
    ("language", pa.string()),
    ("pub_ts_utc", pa.timestamp("ms")),
])

PHRASE_SCHEMA = pa.schema([
    ("news_id", pa.string()),
    ("phrase", pa.string()),
    ("phrase_norm", pa.string()),
    ("mentions", pa.int32()),
    ("max_score", pa.float64()),
    ("text_hash", pa.string()),
    ("language", pa.string()),
    ("pub_ts_utc", pa.timestamp("ms")),
])

SCHEMAS = {"news": NEWS_SCHEMA, "news_entities": ENTITY_SCHEMA, "news_key_phrases": PHRASE_SCHEMA}


def sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def clean_text(value: str | None) -> str:
    if not value:
        return ""
    text = html.unescape(html.unescape(value))
    text = TAG_RE.sub(" ", text)
    return WS_RE.sub(" ", text).strip()


def canonical_url(url: str | None) -> str | None:
    if not url:
        return None
    parts = urlsplit(url.strip())
    if parts.scheme not in ("http", "https") or not parts.netloc:
        return None
    path = parts.path.rstrip("/") or "/"
    return urlunsplit(("https", parts.netloc.lower(), path, "", ""))


def make_news_id(link: str | None, guid: str | None) -> str | None:
    for candidate in (link, guid):
        url = canonical_url(candidate)
        if url:
            m = ARTICLE_ID_RE.search(url)
            if m:
                host = urlsplit(url).netloc.removeprefix("www.")
                return f"{host}-{m.group(1)}"
    base = canonical_url(link) or canonical_url(guid) or (guid or "").strip()
    return f"h-{sha256(base)[:20]}" if base else None


def parse_pub_date(raw: str | None, ref: datetime) -> tuple[datetime | None, str]:
    if not raw or not raw.strip():
        return None, "missing"
    raw = raw.strip()
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        try:
            dt = parsedate_to_datetime(raw)
        except (TypeError, ValueError, IndexError):
            return None, "invalid"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=BOGOTA)
    dt = dt.astimezone(timezone.utc)
    if dt.year < 2000 or dt > ref + timedelta(days=1):
        return None, "invalid"
    return dt.replace(tzinfo=None), "ok"


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def parse_items(content: bytes) -> list[dict]:
    root = ET.fromstring(content)
    items = []
    for i, item in enumerate(root.iter("item")):
        fields, cats = {}, []
        for child in item:
            name = _local(child.tag)
            text = "".join(child.itertext())
            if name == "category":
                if text.strip():
                    cats.append(text.strip())
            elif name not in fields:
                fields[name] = text
        items.append({"item_index": i, "fields": fields, "categories": cats,
                      "raw": ET.tostring(item, encoding="unicode")})
    return items


def normalize_item(item: dict, manifest: dict) -> tuple[dict | None, str | None]:
    f = item["fields"]
    title = clean_text(f.get("title"))
    description = clean_text(f.get("description"))
    link_raw = (f.get("link") or "").strip()
    guid = (f.get("guid") or "").strip()

    reasons = []
    if not title:
        reasons.append("titulo_vacio")
    if not link_raw:
        reasons.append("enlace_faltante")
    elif not canonical_url(link_raw):
        reasons.append("enlace_invalido")
    news_id = make_news_id(link_raw, guid)
    if not news_id:
        reasons.append("sin_identificador")
    if reasons:
        return None, ";".join(reasons)

    ingested = datetime.fromisoformat(manifest["ingested_at_utc"])
    pub_ts, pub_status = parse_pub_date(f.get("pubDate"), ingested)
    nlp_text = f"{title}. {description}" if description else title
    return {
        "news_id": news_id,
        "guid": guid or None,
        "link": canonical_url(link_raw),
        "title": title,
        "description": description,
        "nlp_text": nlp_text,
        "content_hash": sha256(nlp_text),
        "medio": manifest["medio"],
        "seccion": manifest["seccion"],
        "secciones": [manifest["seccion"]],
        "categorias": item["categories"],
        "pub_ts_utc": pub_ts,
        "pub_date_raw": (f.get("pubDate") or "").strip() or None,
        "pub_date_status": pub_status,
        "bronze_uri": manifest["raw_s3_uri"],
        "updated_at_utc": ingested.astimezone(timezone.utc).replace(tzinfo=None),
    }, None


def transform_batch(manifests: list[dict], read_raw) -> tuple[list[dict], list[dict], dict]:
    valid, rejected = [], []
    received = 0
    for m in sorted(manifests, key=lambda x: x["source_id"]):
        if m["status"] != OK:
            continue
        for item in parse_items(read_raw(m)):
            received += 1
            record, reason = normalize_item(item, m)
            if record:
                valid.append(record)
            else:
                rejected.append({"run_id": m["run_id"], "source_id": m["source_id"], "bronze_uri": m["raw_s3_uri"],
                                 "item_index": item["item_index"], "reason": reason, "raw_item": item["raw"]})

    unique: dict[str, dict] = {}
    for r in valid:
        if r["news_id"] in unique:
            kept = unique[r["news_id"]]
            kept["secciones"] = sorted(set(kept["secciones"]) | set(r["secciones"]))
            kept["categorias"] = sorted(set(kept["categorias"]) | set(r["categorias"]))
        else:
            unique[r["news_id"]] = r
    stats = {
        "received": received,
        "valid": len(valid),
        "rejected": len(rejected),
        "unique_in_batch": len(unique),
        "duplicates_in_batch": len(valid) - len(unique),
        "multi_feed_news": sum(len(r["secciones"]) > 1 for r in unique.values()),
    }
    return list(unique.values()), rejected, stats


def merge_into_state(state: dict[str, dict], records: list[dict], run_id: str) -> tuple[dict[str, dict], dict]:
    state = {k: dict(v) for k, v in state.items()}
    counts = {"new": 0, "updated": 0, "repeated": 0, "stale": 0}
    for r in records:
        current = state.get(r["news_id"])
        if current is None:
            state[r["news_id"]] = {**r, "first_seen_run_id": run_id, "last_seen_run_id": run_id,
                                   "last_changed_run_id": run_id, "version": 1, "enrichment_status": "pending"}
            counts["new"] += 1
            continue

        current["secciones"] = sorted(set(current["secciones"]) | set(r["secciones"]))
        current["categorias"] = sorted(set(current["categorias"]) | set(r["categorias"]))
        current["last_seen_run_id"] = max(current["last_seen_run_id"], run_id)

        if run_id < current["last_changed_run_id"]:
            counts["stale"] += 1
        elif r["content_hash"] != current["content_hash"]:
            keep = {k: current[k] for k in ("first_seen_run_id", "last_seen_run_id", "secciones", "categorias")}
            state[r["news_id"]] = {**r, **keep, "seccion": current["seccion"], "last_changed_run_id": run_id,
                                   "version": current["version"] + 1, "enrichment_status": "pending"}
            counts["updated"] += 1
        else:
            counts["repeated"] += 1
    return state, counts


def partition_of(record: dict) -> str:
    ts = record["pub_ts_utc"]
    return ts.strftime("%Y-%m-%d") if ts else UNKNOWN_DATE


def to_parquet_bytes(rows: list[dict], schema: pa.Schema) -> bytes:
    table = pa.Table.from_pylist([{f.name: r.get(f.name) for f in schema} for r in rows], schema=schema)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()


def read_parquet_bytes(data: bytes) -> list[dict]:
    return pq.read_table(io.BytesIO(data)).to_pylist()


def read_pointer(cfg: dict) -> dict | None:
    return s3io.get_json(cfg, POINTER_KEY) if s3io.exists(cfg, POINTER_KEY) else None


def read_state(cfg: dict) -> dict[str, dict]:
    pointer = read_pointer(cfg)
    if not pointer:
        return {}
    rows = []
    for key in s3io.list_keys(cfg, f"{NEWS_PREFIX}version={pointer['version']}/"):
        if key.endswith(".parquet"):
            rows.extend(read_parquet_bytes(s3io.get_bytes(cfg, key)))
    return {r["news_id"]: r for r in rows}


def write_table(cfg: dict, table: str, rows: list[dict], version: str, sort_key: tuple) -> dict[str, str]:
    prefix = f"silver/{table}/version={version}/"
    s3io.delete_prefix(cfg, prefix)
    by_date: dict[str, list[dict]] = {}
    for r in rows:
        by_date.setdefault(partition_of(r), []).append(r)
    locations = {}
    for d, part in sorted(by_date.items()):
        part.sort(key=lambda r: tuple(r[k] for k in sort_key))
        s3io.put_bytes(cfg, f"{prefix}pub_date={d}/part-0.parquet", to_parquet_bytes(part, SCHEMAS[table]))
        locations[d] = f"s3://{cfg['bucket']}/{prefix}pub_date={d}/"
    return locations


def publish_pointer(cfg: dict, version: str, run_id: str, partitions: dict, counts: dict) -> dict:
    pointer = {"version": version, "run_id": run_id, "counts": counts, "partitions": partitions,
               "published_at_utc": datetime.now(timezone.utc).isoformat()}
    s3io.put_json(cfg, POINTER_KEY, pointer)
    return pointer


def cleanup_versions(cfg: dict, keep: int) -> list[str]:
    pointer = read_pointer(cfg)
    removed = []
    for table in TABLES:
        prefix = f"silver/{table}/"
        versions = sorted({k.split("version=")[1].split("/")[0] for k in s3io.list_keys(cfg, prefix)})
        for v in [v for v in versions[:-keep] if not pointer or v != pointer["version"]]:
            s3io.delete_prefix(cfg, f"{prefix}version={v}/")
            removed.append(f"{table}/{v}")
    return removed


def write_quarantine(cfg: dict, run_id: str, rejected: list[dict]) -> str | None:
    key = f"quarantine/silver/run_id={run_id}/rejected.jsonl"
    if not rejected:
        if s3io.exists(cfg, key):
            s3io.delete_prefix(cfg, key)
        return None
    body = "\n".join(json.dumps(r, ensure_ascii=False) for r in rejected).encode("utf-8")
    return s3io.put_bytes(cfg, key, body, "application/x-ndjson")


def _read_raw(cfg: dict):
    def read(manifest: dict) -> bytes:
        key = manifest["raw_s3_uri"].split(f"s3://{cfg['bucket']}/", 1)[1]
        return s3io.get_bytes(cfg, key)
    return read


def list_bronze_run_ids(cfg: dict) -> list[str]:
    ids = {k.split("/run_id=")[1].split("/")[0] for k in s3io.list_keys(cfg, "bronze/") if k.endswith("manifest.json")}
    return sorted(ids)


def build_silver(cfg: dict, run_ids: list[str], from_scratch: bool, attempt: str, comprehend=None) -> dict:
    state = {} if from_scratch else read_state(cfg)
    before = len(state)
    report = {"mode": "rebuild" if from_scratch else "incremental", "runs": {}}
    for run_id in run_ids:
        manifests = load_batch_manifests(cfg, run_id)
        records, rejected, stats = transform_batch(manifests, _read_raw(cfg))
        state, counts = merge_into_state(state, records, run_id)
        write_quarantine(cfg, run_id, rejected)
        report["runs"][run_id] = {**stats, **counts}
        log.info("silver run=%s stats=%s merge=%s", run_id, stats, counts)

    entries, enrichment = enrich.enrich_state(cfg, state, comprehend)
    entity_rows, phrase_rows = enrich.child_rows(state, entries)
    log.info("enrichment=%s entities=%s phrases=%s", enrichment, len(entity_rows), len(phrase_rows))

    version = f"{run_ids[-1]}__{attempt}"
    partitions = {
        "news": write_table(cfg, "news", list(state.values()), version, ("news_id",)),
        "news_entities": write_table(cfg, "news_entities", entity_rows, version, ("news_id", "entity_type", "entity_norm")),
        "news_key_phrases": write_table(cfg, "news_key_phrases", phrase_rows, version, ("news_id", "phrase_norm")),
    }
    counts = {"news": len(state), "news_entities": len(entity_rows), "news_key_phrases": len(phrase_rows)}
    s3io.check_fail_injection(cfg, "silver_before_publish")
    pointer = publish_pointer(cfg, version, run_ids[-1], partitions, counts)
    removed = cleanup_versions(cfg, cfg.get("silver", {}).get("keep_versions", 3))
    report.update({"version": version, "news_before": before, "counts": counts, "enrichment": enrichment,
                   "removed_versions": removed, "published_at_utc": pointer["published_at_utc"]})
    return report
