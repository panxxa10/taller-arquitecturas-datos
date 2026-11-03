"""Definición de entidades de origen: columnas, modo y llave de extracción (sin dependencias pesadas)."""

MYSQL_CSV_COLUMNS = {
    "customers": {"customer_id": "BIGINT", "name": "VARCHAR", "email": "VARCHAR", "phone": "VARCHAR", "city": "VARCHAR", "department": "VARCHAR", "registration_date": "DATE", "updated_at": "TIMESTAMP"},
    "products": {"product_id": "BIGINT", "name": "VARCHAR", "category": "VARCHAR", "price": "DECIMAL(12,2)", "description": "VARCHAR", "is_active": "BOOLEAN", "updated_at": "TIMESTAMP"},
    "orders": {"order_id": "BIGINT", "customer_id": "BIGINT", "order_date": "TIMESTAMP", "status": "VARCHAR", "sales_channel": "VARCHAR", "payment_method": "VARCHAR", "total": "DECIMAL(14,2)"},
    "order_items": {"order_item_id": "BIGINT", "order_id": "BIGINT", "product_id": "BIGINT", "quantity": "INTEGER", "unit_price": "DECIMAL(12,2)", "subtotal": "DECIMAL(14,2)"},
}
MONGO_CSV_COLUMNS = {
    "reviews": {"review_id": "BIGINT", "customer_id": "BIGINT", "product_id": "BIGINT", "rating": "INTEGER", "reviewed_at": "TIMESTAMPTZ", "document_json": "VARCHAR"},
    "navigation_events": {"event_id": "BIGINT", "event_type": "VARCHAR", "occurred_at": "TIMESTAMPTZ", "customer_id": "BIGINT", "product_id": "BIGINT", "sales_channel": "VARCHAR", "document_json": "VARCHAR"},
}


# modo "full": instantánea completa en cada lote (dimensiones pequeñas; captura cambios con fecha retroactiva).
# modo "incremental": solo filas con llave mayor a la marca de agua del último lote confirmado.
EXTRACTION_MODE = {
    ("mysql", "customers"): "full",
    ("mysql", "products"): "full",
    ("mysql", "orders"): "incremental",
    ("mysql", "order_items"): "incremental",
    ("mongodb", "reviews"): "incremental",
    ("mongodb", "navigation_events"): "incremental",
}
