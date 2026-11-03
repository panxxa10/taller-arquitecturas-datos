# Paso 4 – Carga a Redshift y dbt adaptado

## Carga (S3 -> Redshift)
- `ecommerce_dw.loading.redshift_loader` lee los manifiestos confirmados en S3 y carga cada lote con `COPY` usando el rol IAM de Redshift.
- Cada lote se carga en una transacción (Data API `batch_execute_statement`): tabla temporal -> COPY -> DELETE -> INSERT -> registro en `"raw".batch_control`.
- Entidades `full` reemplazan toda la tabla; las incrementales borran por llave y reinsertan (repetir un lote no duplica).
- `batch_control` guarda batch_id, filas esperadas/cargadas y estado (`loaded` o `count_mismatch`). Un lote ya cargado se omite; si no coinciden las filas, el cargador falla.

## Adaptaciones de DuckDB a Redshift
| Local (DuckDB) | Redshift |
|---|---|
| esquema `raw` | `"raw"` siempre entre comillas (palabra reservada) |
| `timezone()` | `convert_timezone()` (macro `to_business_time`) |
| `isodow` | `dow` con 0 -> 7 (macro `iso_day_of_week`) |
| JSON `->>` | `json_extract_path_text(..., true)` (macro `json_text`) |
| `IS DISTINCT FROM` | macro `is_distinct_from` |
| `select * exclude` | `dbt_utils.star(except=...)` |
| diferencia de fechas | `datediff` (macro `days_between`) |

## Ejecución
```bash
# Mac
cd infra/terraform && terraform apply && cd ../..
./scripts/aws/sync_to_ec2.sh
# EC2
./scripts/aws/ec2_run.sh ecommerce_dw.loading.redshift_loader --init --load --status
./scripts/aws/ec2_setup_dbt.sh
./scripts/aws/ec2_dbt.sh deps
./scripts/aws/ec2_dbt.sh seed
./scripts/aws/ec2_dbt.sh run path:models/intermediate
./scripts/aws/ec2_dbt.sh run path:models/quarantine
./scripts/aws/ec2_dbt.sh snapshot
./scripts/aws/ec2_dbt.sh run path:models/marts
./scripts/aws/ec2_dbt.sh test
```
