# Paso 7 – Evidencias de los 6 escenarios y conciliación

Todo se ejecuta en la EC2, desde `~/data-warehouse`. El informe de solo lectura se genera con:
```bash
./scripts/aws/ec2_run.sh ecommerce_dw.orchestration.evidence
```
Imprime corridas del DAG, lotes cargados, cuarentena por motivo y lote, duplicados, `raw = válidas + cuarentena`,
conciliación del indicador de ventas totales (`marts.fct_sales`) y comparación de conteos contra MySQL.

| # | Escenario | Cómo se produce | Qué capturar |
|---|---|---|---|
| 1 | Carga inicial | `trigger` con las fuentes recién generadas (primer lote `initial`) | `ec2_airflow.sh status` (11 tareas `success`) y `redshift_loader --status` |
| 2 | Registros defectuosos | `generate_data.py` inyecta 2 por regla (`--bad-records`); dbt los envía a `quarantine.*` con `rejection_reason` | sección "Registros en cuarentena" del informe |
| 3 | Lote repetido | `./scripts/aws/ec2_airflow.sh trigger <lote ya cargado>` | log de `extract_to_s3` ("ya confirmado… se reutiliza") y conteos del informe sin cambios (sin duplicados) |
| 4 | Lote incremental nuevo | `./scripts/aws/ec2_run.sh ecommerce_dw.sources.append_batch --batch-id 2` y luego `trigger` | `batch_control`: lote nuevo `incremental` con 3 filas por tabla incremental |
| 5 | Cambio de atributo historizado (SCD2) | `simulate_changes` + `trigger`; consulta de `marts.dim_customer` | dos versiones: la anterior con `valid_to` y la nueva con `is_current = true` |
| 6 | Fallo controlado y rerun | `trigger "" load` (falla en la carga) y luego `trigger <lote del fallo>` | corrida `failed` con tareas `upstream_failed`, fila `failed` en `pipeline_run_log`; rerun `success` sin duplicados |

## Fallo controlado y rerun (escenario 6)
```bash
./scripts/aws/ec2_airflow.sh trigger "" load      # falla en load_redshift tras 2 reintentos (~3 min)
./scripts/aws/ec2_airflow.sh status               # corrida failed; las tareas siguientes quedan upstream_failed
./scripts/aws/ec2_run.sh ecommerce_dw.orchestration.evidence | sed -n 1,12p   # pipeline_run_log: estado failed y lote
./scripts/aws/ec2_airflow.sh trigger <lote_del_fallo>   # mismo lote: la extracción se reutiliza y la carga completa
```
El lote del fallo ya está en S3 (la extracción terminó antes del fallo), por eso el rerun no vuelve a consultar las fuentes.

## Conciliación
`evidence` verifica: (a) sin filas duplicadas en `raw`; (b) `raw = válidas + cuarentena` por entidad; (c)
`sum(subtotal)` de `raw.mysql_order_items` = válidas + cuarentena y `marts.fct_sales` = válidas; (d) filas de MySQL = filas
de `raw` (la fuente es la verdad). El indicador del dashboard es `SELECT SUM(subtotal) FROM marts.fct_sales`.
