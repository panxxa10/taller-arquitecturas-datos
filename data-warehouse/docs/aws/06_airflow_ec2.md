# Paso 6 – Orquestación con Airflow en la EC2

Airflow (LocalExecutor + PostgreSQL de metadatos) y dbt corren **en la misma EC2**, en una imagen Docker (`docker/Dockerfile.aws`).
Un solo DAG, `dw_pipeline` (`src/airflow/dags_aws/dw_pipeline.py`), hace todo el flujo sin intervención manual:

`validate_connections → extract_to_s3 → load_redshift → dbt_deps → dbt_seed → dbt_run_intermediate → dbt_run_quarantine → dbt_snapshot → dbt_run_marts → dbt_test → record_result`

| Requisito | Cómo se cumple |
|---|---|
| Validar conexiones y parámetros | `validate_connections` revisa las variables obligatorias y hace S3 `head_bucket`, `SELECT 1` en MySQL y Redshift y `ping` en DocumentDB; informa todos los fallos juntos |
| Dependencias explícitas | Cadena lineal `>>` en el DAG |
| Reintentos acotados | `retries=2`, espera 30 s creciente (máx. 5 min), `execution_timeout` 30 min |
| Logs con corrida y lote | Cada tarea escribe `[dag_run=<corrida> lote=<lote> tarea=<tarea>]` |
| Sin concurrencia | `max_active_runs=1`, `max_active_tasks=1` |
| Resultado registrado | `record_result` (`trigger_rule=all_done`) inserta una fila en `"raw".pipeline_run_log` aunque haya fallos, y luego marca la corrida como fallida si corresponde |
| Lote reproducible | El lote sale del parámetro `batch_id` o, si está vacío, de la hora real de inicio de la corrida (siempre posterior a lotes anteriores; el extractor rechaza lotes más antiguos que el último confirmado); los reintentos reutilizan el lote y no duplican datos |
| Fallo controlado | Parámetro `inject_failure` (`validate`, `extract`, `load`, `dbt_run`, `dbt_test`) |
| Programación | `0 11 * * *` UTC = 06:00 Bogotá, `catchup=False`; el DAG nace pausado y `ec2_airflow.sh up` lo activa |
| Secretos | Ninguno en código ni en la imagen: las contraseñas de las bases se leen de Secrets Manager con el rol de la instancia; los secretos propios de Airflow se generan en la EC2 (`docker/.env.airflow`, fuera de git) |

## Operación (en la EC2, desde `~/data-warehouse`)
```bash
./scripts/aws/ec2_airflow.sh up                          # construir e iniciar
./scripts/aws/ec2_airflow.sh trigger                     # corrida con lote nuevo
./scripts/aws/ec2_airflow.sh trigger 20261103T143345     # repetir un lote ya cargado (no duplica)
./scripts/aws/ec2_airflow.sh trigger "" load             # fallo controlado en la carga
./scripts/aws/ec2_airflow.sh status                      # corridas y tareas
./scripts/aws/ec2_airflow.sh logs load_redshift          # log de una tarea
./scripts/aws/ec2_airflow.sh down
```
La interfaz web solo escucha en `127.0.0.1:8080` de la EC2; se ve con un túnel SSH:
`ssh -i ~/.ssh/taller-dw -L 8080:localhost:8080 ec2-user@<ip>` y luego `http://localhost:8080`.
