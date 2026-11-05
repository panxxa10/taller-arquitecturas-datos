import logging
from collections import Counter, defaultdict
from datetime import datetime, timezone

import pyarrow as pa

from datalake import s3io
from datalake.silver import (SCHEMAS as SILVER_SCHEMAS, partition_of, read_parquet_bytes, read_pointer,
                             to_parquet_bytes)

log = logging.getLogger(__name__)

POINTER_KEY = "gold/_state/current.json"

SCHEMAS = {
    "news_daily": pa.schema([
        ("medio", pa.string()),
        ("seccion", pa.string()),
        ("news_count", pa.int64()),
    ]),
    "entity_daily": pa.schema([
        ("entity_type", pa.string()),
        ("entity_norm", pa.string()),
        ("entity_text", pa.string()),
        ("news_count", pa.int64()),
        ("mentions", pa.int64()),
    ]),
    "key_phrase_section_daily": pa.schema([
        ("seccion", pa.string()),
        ("phrase_norm", pa.string()),
        ("phrase", pa.string()),
        ("news_count", pa.int64()),
    ]),
}


def read_silver_table(cfg: dict, pointer: dict, table: str) -> list[dict]:
    rows = []
    for key in s3io.list_keys(cfg, f"silver/{table}/version={pointer['version']}/"):
        if key.endswith(".parquet"):
            rows.extend(read_parquet_bytes(s3io.get_bytes(cfg, key)))
    return rows


def _display(counter: Counter) -> str:
    return sorted(counter.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]


def aggregate(news: list[dict], entities: list[dict], phrases: list[dict]) -> dict[str, list[dict]]:
    by_id = {n["news_id"]: n for n in news}

    daily = Counter()
    for n in news:
        for sec in n["secciones"]:
            daily[(partition_of(n), n["medio"], sec)] += 1
    news_daily = [{"pub_date": d, "medio": m, "seccion": s, "news_count": c} for (d, m, s), c in daily.items()]

    ent = defaultdict(lambda: {"news": set(), "mentions": 0, "names": Counter()})
    for e in entities:
        if e["news_id"] not in by_id:
            continue
        g = ent[(partition_of(by_id[e["news_id"]]), e["entity_type"], e["entity_norm"])]
        g["news"].add(e["news_id"])
        g["mentions"] += e["mentions"]
        g["names"][e["entity_text"]] += 1
    entity_daily = [{"pub_date": d, "entity_type": t, "entity_norm": k, "entity_text": _display(g["names"]),
                     "news_count": len(g["news"]), "mentions": g["mentions"]} for (d, t, k), g in ent.items()]

    phr = defaultdict(lambda: {"news": set(), "names": Counter()})
    for p in phrases:
        n = by_id.get(p["news_id"])
        if not n:
            continue
        for sec in n["secciones"]:
            g = phr[(partition_of(n), sec, p["phrase_norm"])]
            g["news"].add(p["news_id"])
            g["names"][p["phrase"]] += 1
    key_phrase_section_daily = [{"pub_date": d, "seccion": s, "phrase_norm": k, "phrase": _display(g["names"]),
                                 "news_count": len(g["news"])} for (d, s, k), g in phr.items()]

    return {"news_daily": news_daily, "entity_daily": entity_daily, "key_phrase_section_daily": key_phrase_section_daily}


def write_gold_table(cfg: dict, table: str, rows: list[dict], version: str) -> dict[str, str]:
    prefix = f"gold/{table}/version={version}/"
    s3io.delete_prefix(cfg, prefix)
    by_date: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_date[r["pub_date"]].append(r)
    locations = {}
    schema = SCHEMAS[table]
    for d, part in sorted(by_date.items()):
        part.sort(key=lambda r: tuple(str(r[f.name]) for f in schema))
        s3io.put_bytes(cfg, f"{prefix}pub_date={d}/part-0.parquet", to_parquet_bytes(part, schema))
        locations[d] = f"s3://{cfg['bucket']}/{prefix}pub_date={d}/"
    return locations


def cleanup(cfg: dict, keep: int, current: str) -> list[str]:
    removed = []
    for table in SCHEMAS:
        prefix = f"gold/{table}/"
        versions = sorted({k.split("version=")[1].split("/")[0] for k in s3io.list_keys(cfg, prefix)})
        for v in [v for v in versions[:-keep] if v != current]:
            s3io.delete_prefix(cfg, f"{prefix}version={v}/")
            removed.append(f"{table}/{v}")
    return removed


def build_gold(cfg: dict) -> dict:
    pointer = read_pointer(cfg)
    if not pointer:
        raise ValueError("Silver no tiene una versión publicada")
    news = read_silver_table(cfg, pointer, "news")
    entities = read_silver_table(cfg, pointer, "news_entities")
    phrases = read_silver_table(cfg, pointer, "news_key_phrases")
    tables = aggregate(news, entities, phrases)

    version = pointer["version"]
    partitions = {t: write_gold_table(cfg, t, rows, version) for t, rows in tables.items()}
    counts = {t: len(rows) for t, rows in tables.items()}
    gold_pointer = {"version": version, "silver_version": version, "counts": counts, "partitions": partitions,
                    "published_at_utc": datetime.now(timezone.utc).isoformat()}
    s3io.check_fail_injection(cfg, "gold_before_publish")
    s3io.put_json(cfg, POINTER_KEY, gold_pointer)
    removed = cleanup(cfg, cfg.get("silver", {}).get("keep_versions", 3), version)
    log.info("gold version=%s counts=%s", version, counts)
    return {"version": version, "counts": counts, "removed_versions": removed,
            "news_without_valid_date": sum(partition_of(n) == "unknown" for n in news)}


def published_tables(cfg: dict) -> dict[str, dict]:
    out = {}
    silver_pointer = read_pointer(cfg)
    if silver_pointer:
        for t, parts in silver_pointer["partitions"].items():
            out[f"silver_{t}"] = {"base": f"s3://{cfg['bucket']}/silver/{t}/", "schema": SILVER_SCHEMAS[t],
                                  "partitions": parts}
    if s3io.exists(cfg, POINTER_KEY):
        gold_pointer = s3io.get_json(cfg, POINTER_KEY)
        for t, parts in gold_pointer["partitions"].items():
            out[f"gold_{t}"] = {"base": f"s3://{cfg['bucket']}/gold/{t}/", "schema": SCHEMAS[t], "partitions": parts}
    return out
