from pathlib import Path

import yaml

CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"


def load_pipeline() -> dict:
    return yaml.safe_load((CONFIG_DIR / "pipeline.yaml").read_text(encoding="utf-8"))


def load_sources() -> list[dict]:
    return yaml.safe_load((CONFIG_DIR / "sources.yaml").read_text(encoding="utf-8"))["sources"]
