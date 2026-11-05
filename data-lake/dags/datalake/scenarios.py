"""Herramientas para preparar y revisar los escenarios de prueba.

Uso dentro del contenedor:
    python -m datalake.scenarios lote-defectuoso
    python -m datalake.scenarios lote-modificado
    python -m datalake.scenarios fallo-activar silver_before_publish
    python -m datalake.scenarios fallo-quitar silver_before_publish
    python -m datalake.scenarios estado
    python -m datalake.scenarios lote <run_id>
"""
import hashlib
import json
import re
import sys
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

from datalake import s3io
from datalake.bronze import OK, bronze_prefix, load_batch_manifests
from datalake.config import load_pipeline, load_sources
from datalake.silver import list_bronze_run_ids, read_pointer

STAGES = ("silver_before_publish", "gold_before_publish", "catalog")


def latest_ok_manifest(cfg: dict, source_id: str) -> dict:
    for run_id in reversed(list_bronze_run_ids(cfg)):
        key = bronze_prefix(source_id, run_id) + "manifest.json"
        if s3io.exists(cfg, key):
            m = s3io.get_json(cfg, key)
            if m["status"] == OK and not m.get("controlled_copy"):
                return m
    raise ValueError(f"No hay lotes válidos de {source_id} en Bronze")


def raw_of(cfg: dict, manifest: dict) -> bytes:
    return s3io.get_bytes(cfg, manifest["raw_s3_uri"].split(f"s3://{cfg['bucket']}/", 1)[1])


def make_defective(content: bytes) -> tuple[bytes, list[str]]:
    root = ET.fromstring(content)
    channel = root.find("channel")
    items = channel.findall("item")
    changes = []
    items[0].find("title").text = ""
    changes.append("item 0: título vacío -> titulo_vacio")
    items[1].find("link").text = "enlace-roto-sin-protocolo"
    if items[1].find("guid") is not None:
        items[1].find("guid").text = "enlace-roto-sin-protocolo"
    changes.append("item 1: enlace inválido -> enlace_invalido")
    items[2].find("pubDate").text = "ayer por la tarde"
    changes.append("item 2: fecha inválida -> no se rechaza; pub_date_status=invalid (si la noticia ya existía conserva su fecha válida)")
    channel.append(ET.fromstring(ET.tostring(items[3])))
    changes.append("item 3: duplicado dentro del mismo feed -> duplicates_in_batch")
    return ET.tostring(root, encoding="utf-8", xml_declaration=True), changes


def make_modified(content: bytes) -> tuple[bytes, list[str]]:
    root = ET.fromstring(content)
    item = root.find("channel").findall("item")[0]
    old = item.find("title").text or ""
    item.find("title").text = f"{old} (ACTUALIZADO)"
    desc = item.find("description")
    if desc is not None:
        desc.text = (desc.text or "") + " Nota actualizada con nueva información."
    return ET.tostring(root, encoding="utf-8", xml_declaration=True), [f"item 0: título cambiado: '{old}' -> '{old} (ACTUALIZADO)'"]


def create_controlled_batch(kind: str, target_source: str | None = None) -> str:
    cfg = load_pipeline()
    sources = load_sources()
    target = target_source or sources[0]["id"]
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    run_id = f"{stamp}_{'test' + kind[:3]}"
    for src in sources:
        origin = latest_ok_manifest(cfg, src["id"])
        content = raw_of(cfg, origin)
        changes = []
        if src["id"] == target:
            content, changes = (make_defective if kind == "defectuoso" else make_modified)(content)
        prefix = bronze_prefix(src["id"], run_id)
        uri = s3io.put_bytes(cfg, prefix + "feed.xml", content, "application/rss+xml")
        manifest = {**origin, "run_id": run_id, "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
                    "content_sha256": hashlib.sha256(content).hexdigest(), "content_bytes": len(content),
                    "item_count": content.count(b"<item>"), "raw_s3_uri": uri, "controlled_copy": True,
                    "copied_from": origin["raw_s3_uri"], "test_kind": kind, "test_changes": changes}
        s3io.put_json(cfg, prefix + "manifest.json", manifest)
        print(f"{src['id']}: copiado de {origin['run_id']}" + (f" con cambios: {changes}" if changes else " sin cambios"))
    print(f"\nLote controlado creado: {run_id}")
    print(f"En Airflow: Trigger con reprocess_run_id = {run_id}")
    return run_id


def show_status():
    cfg = load_pipeline()
    pointer = read_pointer(cfg) or {}
    gold = s3io.get_json(cfg, "gold/_state/current.json") if s3io.exists(cfg, "gold/_state/current.json") else {}
    flags = [k.removeprefix(s3io.FAIL_PREFIX) for k in s3io.list_keys(cfg, s3io.FAIL_PREFIX)]
    print("Silver vigente:", pointer.get("version"), pointer.get("counts"))
    print("Gold vigente:  ", gold.get("version"), gold.get("counts"))
    print("Fallos activos:", flags or "ninguno")
    print("Lotes en Bronze:", list_bronze_run_ids(cfg)[-8:])


def show_run(run_id: str):
    cfg = load_pipeline()
    for m in load_batch_manifests(cfg, run_id):
        print(f"manifest {m['source_id']}: status={m['status']} items={m.get('item_count')} "
              f"sha256={str(m.get('content_sha256'))[:12]} controlado={m.get('controlled_copy', False)}")
    for key in sorted(s3io.list_keys(cfg, f"control/runs/run_id={run_id}/")):
        rec = s3io.get_json(cfg, key)
        print(f"\n--- {key}")
        if "runs" in rec:
            print(json.dumps({k: v for k, v in rec.items() if k != "runs"}, ensure_ascii=False, default=str))
            print("detalle por lote:", json.dumps(rec["runs"], ensure_ascii=False))
        elif "stages" in rec:
            print(json.dumps({k: v for k, v in rec.items() if k != "stages"}, ensure_ascii=False, default=str))
        else:
            print(json.dumps(rec, ensure_ascii=False, default=str))
    q = f"quarantine/silver/run_id={run_id}/rejected.jsonl"
    if s3io.exists(cfg, q):
        print(f"\n--- {q}")
        for line in s3io.get_bytes(cfg, q).decode("utf-8").splitlines():
            r = json.loads(line)
            print(f"{r['source_id']} item={r['item_index']} motivo={r['reason']} bronze={r['bronze_uri']}")


def main(argv: list[str]):
    cmd = argv[0] if argv else "estado"
    cfg = load_pipeline()
    if cmd == "lote-defectuoso":
        create_controlled_batch("defectuoso", argv[1] if len(argv) > 1 else None)
    elif cmd == "lote-modificado":
        create_controlled_batch("modificado", argv[1] if len(argv) > 1 else None)
    elif cmd in ("fallo-activar", "fallo-quitar"):
        stage = argv[1] if len(argv) > 1 else "silver_before_publish"
        if stage not in STAGES:
            raise SystemExit(f"Etapa inválida. Opciones: {STAGES}")
        key = f"{s3io.FAIL_PREFIX}{stage}"
        if cmd == "fallo-activar":
            s3io.put_bytes(cfg, key, b"fallo controlado")
            print(f"Fallo activado en {stage}. El próximo run fallará en esa etapa.")
        else:
            s3io.delete_prefix(cfg, key)
            print(f"Fallo quitado de {stage}. Usa 'Clear' en la tarea fallida para reejecutar.")
    elif cmd == "estado":
        show_status()
    elif cmd == "lote" and len(argv) > 1:
        show_run(argv[1])
    else:
        print(__doc__)


if __name__ == "__main__":
    main(sys.argv[1:])
