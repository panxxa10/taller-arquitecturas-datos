# Paso 3: extracción inicial e incremental a S3

Código: [`s3_extract.py`](../../src/ecommerce_dw/ingestion/s3_extract.py) · Entidades: [`entities.py`](../../src/ecommerce_dw/ingestion/entities.py) · Pruebas: [`tests/test_s3_extract.py`](../../tests/test_s3_extract.py)

## Organización en S3

```
s3://<bucket>/raw/<fuente>/<entidad>/batch_id=<AAAAMMDDTHHMMSS>/part-0000.csv.gz
s3://<bucket>/raw/<fuente>/<entidad>/batch_id=<AAAAMMDDTHHMMSS>/_manifest.json
```
Fuentes: `mysql` (customers, products, orders, order_items) y `mongodb` (reviews, navigation_events).

## Qué se registra en cada lote (`_manifest.json`)

`batch_id`, `extracted_at` (UTC), `rows`, `batch_type` (initial / incremental / full_snapshot), `watermark_from` → `watermark_to`, archivos con `bytes` y `sha256`, `run_id` de Airflow, versión del extractor y `status`. Cada fila del CSV lleva además `_batch_id` y `_extracted_at`.

**Cómo se identifica un lote procesado:** el lote queda *confirmado* cuando existe su `_manifest.json`, que se escribe **al final**. Sin manifiesto el intento se considera incompleto y se rehace. En Redshift (paso 4) la tabla `raw.batch_control` registra cuándo se cargó cada lote.

## Reglas

| Requisito del taller | Cómo se cumple |
|---|---|
| Bucket privado | Bloqueo de acceso público, cifrado AES256 y versionado (Terraform) |
| Fuente, entidad y lote en la ruta | `raw/<fuente>/<entidad>/batch_id=<id>/` |
| Fecha, lote y cantidad de registros | `_manifest.json` y columnas `_batch_id`, `_extracted_at` |
| Reprocesar sin consultar las fuentes | Repetir un lote confirmado lee el manifiesto y no abre MySQL ni DocumentDB |
| No sobrescribir silenciosamente otros lotes | El `batch_id` forma parte de la ruta; un `batch_id` anterior al último confirmado se rechaza; el rol de la EC2 no tiene `s3:DeleteObject`; versionado activo |

## Inicial vs. incremental

| Entidad | Modo | Criterio |
|---|---|---|
| `orders`, `order_items`, `reviews`, `navigation_events` | incremental | Llave > marca de agua del último lote confirmado. El primer lote es la carga inicial |
| `customers`, `products` | instantánea completa | Son pequeñas y `simulate_changes.py` escribe `updated_at` con una fecha **retroactiva** (p. ej. 2025-07-01): una marca de agua sobre `updated_at` no vería ese cambio |

Limitación: la marca de agua por llave no detecta filas con llave menor a la ya extraída (lotes aplicados fuera de orden). Se evita aplicando lotes en orden creciente.

## Formato: CSV gzip

`COPY` de Redshift lo carga directamente (`CSV GZIP IGNOREHEADER 1`) y reutiliza la extracción CSV del proyecto original. Los documentos de MongoDB viajan como JSON en una sola columna de texto con comillas, y `dbt` los convierte a `SUPER` en Redshift. Parquet conservaría tipos, pero añade `pyarrow` y la conversión de timestamps con zona sin ganancia para estos volúmenes.

## Ejecución (en la EC2, desde `~/data-warehouse`)

```bash
# 1. Carga inicial (primer lote)
./scripts/aws/ec2_run.sh ecommerce_dw.ingestion.s3_extract

# 2. Idempotencia: repetir el MISMO lote no consulta las fuentes (reutilizado = sí)
./scripts/aws/ec2_run.sh ecommerce_dw.ingestion.s3_extract --batch-id <el id impreso en el paso 1>

# 3. Nuevo lote incremental: añadir datos a las fuentes y extraer
./scripts/aws/ec2_run.sh ecommerce_dw.sources.append_batch --batch-id 1
./scripts/aws/ec2_run.sh ecommerce_dw.ingestion.s3_extract

# 4. Cambio en un atributo historizado (SCD2): lo captura la instantánea de customers/products
./scripts/aws/ec2_run.sh ecommerce_dw.sources.simulate_changes --customer-id 1 --city Medellín --department Antioquia --product-id 1 --category Hogar --changed-at 2025-07-01
./scripts/aws/ec2_run.sh ecommerce_dw.ingestion.s3_extract

# Lotes confirmados
./scripts/aws/ec2_run.sh ecommerce_dw.ingestion.s3_extract --list
aws s3 ls s3://$(grep S3_BUCKET .env.aws | cut -d= -f2)/raw/ --recursive | head -30
```

Resultado esperado: el primer lote tiene `initial` en las 6 entidades (filas = conteos del paso 2: 54, 27, 106, 259, 158, 508); el segundo, `incremental` con **3 filas** en orders, reviews y navigation_events y filas de `order_items` del lote.

## Pruebas automáticas (sin tocar AWS)

`pip install boto3 moto pytest pymongo pymysql && PYTHONPATH=src pytest tests`: carga inicial con manifiesto y metadatos, repetición sin consultar la fuente, incremental con marca de agua, lote vacío, fallo y reintento sin manifiesto parcial, y rechazo de lotes anteriores.
