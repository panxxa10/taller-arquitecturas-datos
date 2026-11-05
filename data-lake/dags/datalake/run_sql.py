import sys
from pathlib import Path

from datalake.athena import run_query
from datalake.config import load_pipeline


def main(paths: list[str]):
    cfg = load_pipeline()
    for path in paths:
        sql = Path(path).read_text(encoding="utf-8")
        res = run_query(cfg, sql)
        print(f"\n=== {Path(path).name} | query_id={res['query_id']} | bytes_scanned={res['bytes_scanned']:,}")
        widths = [max(len(str(c or "")) for c in col) for col in zip(res["columns"], *res["rows"][:20])]
        for row in [res["columns"], *res["rows"][:20]]:
            print("  ".join(str(c or "").ljust(w)[:60] for c, w in zip(row, widths)))
        if len(res["rows"]) > 20:
            print(f"... ({len(res['rows'])} filas en total)")


if __name__ == "__main__":
    main(sys.argv[1:] or sorted(str(p) for p in (Path(__file__).parent.parent / "sql").glob("*.sql")))
