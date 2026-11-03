#!/usr/bin/env python3
"""Crea las tablas de MySQL (RDS) con schema.sql. Es idempotente (CREATE TABLE IF NOT EXISTS)."""

from pathlib import Path

from ecommerce_dw.sources.generate_data import connect_mysql

SCHEMA = Path(__file__).parent / "mysql" / "schema.sql"


def main():
    statements = [s.strip() for s in SCHEMA.read_text(encoding="utf-8").split(";") if s.strip()]
    connection = connect_mysql()
    try:
        with connection.cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)
        connection.commit()
    finally:
        connection.close()
    print(f"MySQL: {len(statements)} tablas listas")


if __name__ == "__main__":
    main()
