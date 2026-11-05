# 3. Reglas de calidad, identificación y actualización

## Identificación de noticias

Los enlaces de El Tiempo terminan en un número de artículo, por ejemplo `...-3598267`. El `news_id` se arma con el dominio y ese número: `eltiempo.com-3598267`.

Orden de prioridad:
1. Número de artículo extraído del enlace.
2. Número de artículo extraído del GUID.
3. Si ninguno tiene número, un hash SHA-256 del enlace normalizado (`h-<hash>`).

Antes de extraer el número, el enlace se normaliza: https, dominio en minúsculas, sin parámetros (`?utm=...`), sin fragmento y sin barra final.

Ventaja: si El Tiempo cambia el título y con él el texto de la URL, el número se mantiene y la noticia no se duplica.

Limitaciones:
- Depende de que el medio conserve el número de artículo. Si lo cambiara, la noticia se contaría dos veces.
- Si un enlace no tiene número, la identidad depende del enlace completo. Un cambio en la URL generaría una noticia nueva.
- No se intenta detectar la misma noticia publicada por medios distintos con enlaces distintos.

## Noticias presentes en varios feeds

Una noticia puede salir en más de una sección, por ejemplo política y justicia. En Silver queda una sola fila con `secciones` igual a la lista de todas sus secciones. `seccion` guarda la primera en la que apareció. En Gold, la noticia cuenta una vez en cada sección. El indicador `multi_feed_news` del registro de cada lote informa cuántas noticias llegaron por más de un feed.

## Validaciones por registro (Silver)

| Regla | Si falla |
|---|---|
| Título no vacío después de limpiar HTML | Rechazo: `titulo_vacio` |
| Enlace presente | Rechazo: `enlace_faltante` |
| Enlace válido (http o https y dominio) | Rechazo: `enlace_invalido` |
| Identificador derivable | Rechazo: `sin_identificador` |
| Fecha de publicación presente y válida | No se rechaza. `pub_date_status = missing` o `invalid` y partición `unknown` |
| Fecha no posterior a 1 día después de la ingesta ni anterior al año 2000 | Se trata como `invalid` |

Un registro puede tener varios motivos separados por `;`. Los rechazos se guardan en cuarentena con su motivo y la referencia al archivo y al item de Bronze.

## Limpieza

- Se decodifican las entidades HTML (dos veces, para casos como `&amp;lt;`).
- Se eliminan las etiquetas HTML y se normalizan los espacios.
- Las fechas se convierten a UTC. Se aceptan ISO 8601 y RFC 822. Una fecha sin zona horaria se interpreta en hora de Colombia (UTC-5). El valor original queda en `pub_date_raw`.

## Deduplicación y actualización

Dentro de un lote, la misma noticia en dos feeds o dos veces en un feed queda en una sola fila. El número se reporta como `duplicates_in_batch`.

Entre lotes, cada noticia del lote se compara con el estado vigente de Silver:

| Clasificación | Condición | Efecto |
|---|---|---|
| new | El `news_id` no existe | Se agrega con `version = 1` |
| repeated | Mismo `content_hash` | Solo se actualizan `last_seen_run_id` y la lista de secciones |
| updated | Cambia `content_hash` y el lote es más reciente que la última modificación | Se reemplaza el contenido, `version + 1` y se marca para enriquecer |
| stale | El lote es más antiguo que la última modificación de la noticia | Se ignora el contenido para no retroceder a una versión vieja |

`content_hash` se calcula sobre el texto que analiza Comprehend (título y descripción). Así, una noticia solo se vuelve a enriquecer cuando cambia ese texto.

Una noticia que sale del feed se conserva en Silver. El RSS es una ventana de las últimas 10 noticias, y salir de ella no significa que la noticia deje de existir. No se conserva el historial de versiones anteriores, que es opcional en el taller. Silver guarda solo la versión vigente, con su número de versión y los lotes de primera y última aparición.

## Enriquecimiento con Comprehend

Se analiza `nlp_text` en español (`es`). Para cada texto:

| Estado | Condición | Llama a Comprehend |
|---|---|---|
| skipped_empty | Menos de 20 caracteres | No |
| skipped_too_long | Más de 4.500 bytes UTF-8 | No |
| unsupported_language | DetectDominantLanguage devuelve otro idioma con puntaje ≥ 0,5 | Solo detección de idioma |
| enriched | Análisis completo | DetectDominantLanguage, DetectEntities y DetectKeyPhrases |
| error | Error del servicio después de 5 intentos | Se reintenta en la próxima ejecución. No se guarda en caché |

Antes de llamar al servicio se consulta la caché por hash del texto. Si el texto ya se analizó, se reutiliza el resultado.

Solo se conservan las entidades PERSON, ORGANIZATION y LOCATION. Las demás (DATE, QUANTITY, TITLE, etc.) se descartan. En las frases clave se quita el artículo inicial y se descartan las frases de menos de 3 caracteres o compuestas solo por números.

### Limitaciones del enriquecimiento

- Comprehend solo ve el título y la descripción, que son textos cortos y sin contexto. Por eso comete errores de tipo. Ejemplo observado: "Petro" se clasificó como ORGANIZATION y no como PERSON.
- Sustantivos genéricos como "Gobierno", "Senado" o "Cámara" se detectan como organizaciones y ocupan los primeros lugares del ranking.
- No se resuelven alias. "Gustavo Petro" y "Petro" cuentan como entidades distintas. El taller no lo exige.
- El análisis depende de la versión del modelo de Comprehend. El mismo texto podría dar resultados distintos en el futuro. La caché fija el resultado del primer análisis.

## Validaciones del lote (Athena)

La tarea `validate_with_athena` ejecuta estas consultas en cada lote. Todas deben devolver 0 fallas:

| Validación | Qué revisa |
|---|---|
| news_id_unico | No hay `news_id` repetidos en el conjunto vigente |
| campos_obligatorios | news_id, title, link y medio no son nulos ni vacíos |
| fechas_y_enlaces_validos | El enlace es http(s), la partición coincide con `pub_ts_utc` y `unknown` solo tiene fechas nulas |
| entidades_sin_noticia | Toda entidad corresponde a una noticia vigente con el mismo texto |
| frases_sin_noticia | Igual para frases clave |
| conciliacion_noticias_gold_silver | `gold_news_daily` coincide con el conteo directo en Silver por día y sección |
| conciliacion_entidades_gold_silver | `gold_entity_daily` coincide con Silver por día y entidad |

La identificación de registros nuevos, repetidos, actualizados y rechazados se registra por lote en `control/runs/run_id=<lote>/silver_*.json`.

## Registros recibidos y noticias únicas

Para cada lote se registra:
- `received`: items leídos de los feeds (10 por feed).
- `valid` y `rejected`.
- `unique_in_batch`: noticias distintas dentro del lote.
- `duplicates_in_batch`: repeticiones dentro del lote.
- `new`, `updated`, `repeated` y `stale`: resultado frente al estado vigente.

La diferencia entre recibidos y noticias únicas publicadas tiene tres causas: los rechazos, las repeticiones dentro del lote (la misma noticia en varios feeds) y, sobre todo, las repeticiones entre lotes. Como cada lote es una ventana de las últimas 10 noticias por feed, la mayoría de los items de un lote ya se habían recibido en el anterior.
