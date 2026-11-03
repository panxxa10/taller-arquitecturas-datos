#!/usr/bin/env python3
"""Cuenta filas/documentos en RDS MySQL y DocumentDB para comprobar la migración."""

import os

from pymongo import MongoClient

from ecommerce_dw.aws_config import get_mongodb_uri
from ecommerce_dw.sources.generate_data import connect_mysql

TABLES = ["customers", "products", "orders", "order_items"]
COLLECTIONS = ["reviews", "navigation_events"]


def main():
    connection = connect_mysql()
    try:
        with connection.cursor() as cursor:
            for table in TABLES:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                print(f"MySQL (RDS)   {table:<18}{cursor.fetchone()[0]:>8}")
    finally:
        connection.close()

    client = MongoClient(get_mongodb_uri())
    try:
        database = client[os.getenv("MONGO_DATABASE", "ecommerce")]
        for name in COLLECTIONS:
            print(f"DocumentDB    {name:<18}{database[name].count_documents({}):>8}")
        info = client.server_info()
        print(f"Servidor DocumentDB (compatibilidad MongoDB): {info.get('version')}")
    finally:
        client.close()


if __name__ == "__main__":
    main()
