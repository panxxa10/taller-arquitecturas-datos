import logging
import re
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from datalake import s3io

log = logging.getLogger(__name__)

CACHE_PREFIX = "silver/_enrichment_cache/"
ENTITY_TYPES = {"PERSON", "ORGANIZATION", "LOCATION"}
FINAL_STATUSES = {"enriched", "skipped_empty", "skipped_too_long", "unsupported_language"}


def comprehend_client(cfg: dict):
    return boto3.client("comprehend", region_name=cfg["region"],
                        config=Config(retries={"mode": "standard", "max_attempts": 5}))


def cache_key(text_hash: str) -> str:
    return f"{CACHE_PREFIX}text_hash={text_hash}.json"


def precheck(text: str, ccfg: dict) -> str | None:
    if len(text.strip()) < ccfg.get("min_text_chars", 20):
        return "skipped_empty"
    if len(text.encode("utf-8")) > ccfg.get("max_text_bytes", 4500):
        return "skipped_too_long"
    return None


def analyze(client, text: str, text_hash: str, ccfg: dict) -> dict:
    entry = {"text_hash": text_hash, "text": text, "language": ccfg["language"],
             "analyzed_at_utc": datetime.now(timezone.utc).isoformat(), "entities": [], "key_phrases": []}
    status = precheck(text, ccfg)
    if status:
        return {**entry, "status": status}

    langs = client.detect_dominant_language(Text=text)["Languages"]
    top = max(langs, key=lambda x: x["Score"]) if langs else None
    entry["detected_language"] = top["LanguageCode"] if top else None
    entry["detected_language_score"] = round(top["Score"], 4) if top else None
    if top and top["LanguageCode"] != ccfg["language"] and top["Score"] >= 0.5:
        return {**entry, "status": "unsupported_language"}

    entities = client.detect_entities(Text=text, LanguageCode=ccfg["language"])["Entities"]
    phrases = client.detect_key_phrases(Text=text, LanguageCode=ccfg["language"])["KeyPhrases"]
    entry["entities"] = [{"type": e["Type"], "text": e["Text"], "score": round(e["Score"], 4)} for e in entities]
    entry["key_phrases"] = [{"text": p["Text"], "score": round(p["Score"], 4)} for p in phrases]
    return {**entry, "status": "enriched"}


def enrich_state(cfg: dict, state: dict[str, dict], client=None) -> tuple[dict[str, dict], dict]:
    ccfg = cfg["comprehend"]
    cached = {k.split("text_hash=")[1].removesuffix(".json") for k in s3io.list_keys(cfg, CACHE_PREFIX)}
    entries: dict[str, dict] = {}
    counts = {"already_enriched": 0, "cache_hits": 0, "api_calls": 0, "errors": 0}

    for news in state.values():
        h = news["content_hash"]
        if news.get("enriched_text_hash") == h and news.get("enrichment_status") in FINAL_STATUSES:
            counts["already_enriched"] += 1
        elif h in entries or h in cached:
            counts["cache_hits"] += 1
        else:
            try:
                client = client or comprehend_client(cfg)
                entries[h] = analyze(client, news["nlp_text"], h, ccfg)
                s3io.put_json(cfg, cache_key(h), entries[h])
                counts["api_calls"] += 1
            except (ClientError, BotoCoreError) as e:
                log.error("news=%s error Comprehend: %s", news["news_id"], e)
                news["enrichment_status"] = "error"
                counts["errors"] += 1
                continue

        entry = entries.get(h) or s3io.get_json(cfg, cache_key(h))
        entries[h] = entry
        news["enrichment_status"] = entry["status"]
        news["enriched_text_hash"] = h
        news["enrichment_language"] = entry["language"]
        news["enriched_at_utc"] = datetime.fromisoformat(entry["analyzed_at_utc"]).astimezone(timezone.utc).replace(tzinfo=None)

    status_counts: dict[str, int] = {}
    for news in state.values():
        status_counts[news["enrichment_status"]] = status_counts.get(news["enrichment_status"], 0) + 1
    counts["status"] = status_counts
    return entries, counts


EDGE_CHARS = "'\"‘’“”«»¿?¡!.,;:()[]{}-–—|"
LEADING_ARTICLE = re.compile(r"^(el|la|los|las|lo|un|una|unos|unas|al|del)\s+", re.IGNORECASE)
ONLY_NUMBERS = re.compile(r"^[\d\W_]+$")


def _norm(text: str) -> str:
    return " ".join(text.split()).strip(EDGE_CHARS + " ")


def _phrase(text: str) -> str:
    return LEADING_ARTICLE.sub("", _norm(text)).strip()


def _useful_phrase(text: str) -> bool:
    return len(text) >= 3 and not ONLY_NUMBERS.match(text)


def child_rows(state: dict[str, dict], entries: dict[str, dict]) -> tuple[list[dict], list[dict]]:
    entity_rows, phrase_rows = [], []
    for news in state.values():
        entry = entries.get(news.get("enriched_text_hash"))
        if not entry or news.get("enrichment_status") != "enriched" or entry["text_hash"] != news["content_hash"]:
            continue
        base = {"news_id": news["news_id"], "pub_ts_utc": news["pub_ts_utc"], "text_hash": entry["text_hash"],
                "language": entry["language"]}

        grouped: dict[tuple, dict] = {}
        for e in entry["entities"]:
            if e["type"] not in ENTITY_TYPES or not _norm(e["text"]):
                continue
            key = (e["type"], _norm(e["text"]).lower())
            g = grouped.setdefault(key, {**base, "entity_type": e["type"], "entity_text": _norm(e["text"]),
                                         "entity_norm": key[1], "mentions": 0, "max_score": 0.0})
            g["mentions"] += 1
            g["max_score"] = max(g["max_score"], e["score"])
        entity_rows.extend(grouped.values())

        grouped = {}
        for p in entry["key_phrases"]:
            phrase = _phrase(p["text"])
            if not _useful_phrase(phrase):
                continue
            key = phrase.lower()
            g = grouped.setdefault(key, {**base, "phrase": phrase, "phrase_norm": key,
                                         "mentions": 0, "max_score": 0.0})
            g["mentions"] += 1
            g["max_score"] = max(g["max_score"], p["score"])
        phrase_rows.extend(grouped.values())
    return entity_rows, phrase_rows
