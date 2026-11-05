# 4. Incrementalidad y recuperación

## Identificación de un lote

Cada ejecución del DAG tiene un `run_id` de Airflow. El lote se identifica con un `batch_id` derivado de forma determinista: la marca de tiempo del `run_id` y los 6 primeros caracteres de su SHA-1. Ejemplo: `20261104T213058_ea4d01`.

Como es determinista, un reintento o una reejecución de la misma corrida de Airflow usa el mismo lote y escribe en las mismas rutas de Bronze. No crea una copia lógica nueva.

## Modos de ejecución

| Modo | Cómo se activa | Qué hace |
|---|---|---|
| Nuevo | Parámetros vacíos (manual o programado) | Consulta los RSS, crea un lote en Bronze y lo integra a Silver |
| Reprocesamiento | `reprocess_run_id = <lote>` | No consulta los RSS. Lee el lote desde Bronze y lo vuelve a integrar |
| Reconstrucción | `rebuild_from_bronze = true` | No consulta los RSS. Arma Silver desde cero con todos los lotes de Bronze en orden |

Reprocesar un lote no crea otra copia en Bronze. Solo se escriben registros nuevos en `control/` que indican el modo usado.

## Marca de avance y estado del lote

Los estados se escriben en estos momentos:

1. `ingest` escribe `bronze/.../manifest.json` de cada fuente al terminar su descarga.
2. `register_ingestion` escribe `control/runs/run_id=<lote>/ingestion_<modo>_<hora>.json` con el resultado de la ingesta: complete, partial o failed.
3. `build_silver` publica `silver/_state/current.json`. Esta es la marca de avance del data lake: indica qué versión de Silver está vigente y cuál fue el último lote integrado. También escribe `silver_<modo>_<hora>.json` con los conteos.
4. `build_gold` publica `gold/_state/current.json`.
5. `finalize` escribe `final_<modo>_<hora>.json` con el estado final del lote y el resultado de cada etapa. Se ejecuta aunque una etapa anterior falle (`trigger_rule = all_done`).

Un lote se considera procesado cuando existe su registro `final_*.json` con estado complete o partial.

## Fuentes que fallan

Cada fuente se descarga en una tarea separada. Los errores de red y los HTTP 429, 500, 502, 503 y 504 se consideran transitorios: la tarea lanza una excepción y Airflow reintenta (2 reintentos). En el último intento la tarea no falla. Registra la fuente con su estado de error en el manifiesto.

El resultado del lote es:
- complete: todas las fuentes en `ok`.
- partial: al menos una en `ok` y al menos una con error.
- failed: ninguna en `ok`. `register_ingestion` falla y las etapas siguientes no se ejecutan.

Una fuente caída queda registrada con `status = http_error` o `network_error`. Nunca aparece como una fuente sin noticias, porque ese caso tiene su propio estado (`no_items`).

## Escritura sin resultados parciales visibles

1. Silver y Gold se escriben en una carpeta nueva `version=<lote>__<hora>`. Las versiones anteriores no se tocan.
2. Solo cuando todos los archivos están escritos se actualiza el puntero `_state/current.json`. Es una sola escritura en S3, que es atómica.
3. `update_catalog` mueve las particiones de Glue a la versión nueva. Hasta ese momento Athena sigue leyendo la versión anterior completa.
4. Se conservan las 3 últimas versiones, así que una partición de Glue nunca apunta a una carpeta borrada.

Si una ejecución falla antes del paso 2, la carpeta incompleta queda huérfana pero nadie la lee. Luego se elimina por la limpieza de versiones.

## Recuperación ante fallos

1. Airflow reintenta cada tarea hasta 2 veces.
2. Si la tarea falla definitivamente, `finalize` registra el lote como failed con la lista de etapas sin resultado.
3. Para recuperar, se corrige la causa y se usa **Clear** sobre la tarea fallida en Airflow. La tarea se vuelve a ejecutar con el mismo lote.
4. Como cada etapa recalcula su salida a partir de su entrada y publica una versión nueva, la reejecución no duplica resultados.

Otra opción es lanzar un reprocesamiento con `reprocess_run_id` del lote afectado.

## Control de concurrencia

`max_active_runs = 1` impide que dos ejecuciones del DAG escriban a la vez sobre Silver, Gold o el catálogo. Una ejecución programada que coincide con una manual espera su turno.

## Incrementalidad

- RSS: solo se consultan en el modo nuevo.
- Comprehend: solo se llama para textos que no están en la caché. Una noticia repetida no genera llamadas. Una noticia modificada genera una llamada nueva solo si cambió su título o su descripción. La reconstrucción desde Bronze reutiliza la caché.
- Silver y Gold: se recalculan completos en cada ejecución porque el volumen es pequeño. Esto no requiere volver a consultar fuentes ni servicios externos.

## Actualización de metadatos

`update_catalog` compara las particiones registradas en Glue con las del puntero vigente:
- Crea las particiones nuevas, por ejemplo cuando aparece una fecha de publicación nueva.
- Actualiza la ubicación de las existentes hacia la versión nueva.
- Elimina las que ya no existen.

No se usa crawler. El esquema de cada tabla se define en código a partir del esquema de Parquet.
