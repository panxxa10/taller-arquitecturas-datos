# 2. Fuentes y diccionario de datos

## Fuentes RSS

Se tomaron del directorio RSS de El Tiempo. La configuración está en `dags/config/sources.yaml`.

| id | Medio | Sección | URL del feed |
|---|---|---|---|
| eltiempo_politica | El Tiempo | politica | https://www.eltiempo.com/rss/politica.xml |
| eltiempo_economia | El Tiempo | economia | https://www.eltiempo.com/rss/economia.xml |
| eltiempo_justicia | El Tiempo | justicia | https://www.eltiempo.com/rss/justicia.xml |

Cada feed publica las 10 noticias más recientes. Cada `<item>` trae `title`, `link`, `guid` (igual al enlace), `pubDate` en ISO 8601 con zona horaria, `category` (subsección), `description` en texto plano y `enclosure` (imagen). Solo se usa el contenido del RSS. No se descargan las páginas enlazadas.

## Bronze

### `manifest.json` (uno por fuente y lote)

| Campo | Descripción |
|---|---|
| run_id | Identificador del lote |
| source_id, medio, seccion, url | Fuente consultada |
| ingested_at_utc | Fecha y hora de la ingesta en UTC |
| http_status | Estado HTTP de la respuesta |
| status | ok, empty_response, http_error, network_error, malformed_xml, no_items |
| error | Detalle del error, si hubo |
| content_sha256 | Hash del contenido |
| content_bytes, item_count | Tamaño y número de items |
| raw_s3_uri | Ubicación del XML original en S3 |
| controlled_copy, copied_from | Solo en lotes de prueba: indica que es una copia controlada y su origen |

## Silver

### `silver_news`: una fila por noticia vigente

| Columna | Tipo | Descripción |
|---|---|---|
| news_id | string | Identificador estable (ver reglas de identificación) |
| guid | string | GUID original del RSS |
| link | string | Enlace normalizado (https, sin parámetros ni fragmento) |
| title | string | Título sin HTML |
| description | string | Descripción sin HTML |
| nlp_text | string | Texto analizado por Comprehend: título + descripción |
| content_hash | string | SHA-256 de `nlp_text` |
| medio | string | Medio de la fuente |
| seccion | string | Primera sección en la que se vio la noticia |
| secciones | array&lt;string&gt; | Todas las secciones en las que apareció |
| categorias | array&lt;string&gt; | Valores de `<category>` del RSS |
| pub_ts_utc | timestamp | Fecha de publicación en UTC |
| pub_date_raw | string | Fecha original del RSS |
| pub_date_status | string | ok, missing o invalid |
| first_seen_run_id | string | Primer lote en el que apareció |
| last_seen_run_id | string | Último lote en el que apareció |
| last_changed_run_id | string | Lote de la última modificación |
| version | int | Número de versión del contenido. Sube cuando cambia el texto |
| bronze_uri | string | Archivo de Bronze de la versión vigente |
| updated_at_utc | timestamp | Ingesta de la versión vigente |
| enrichment_status | string | Estado del enriquecimiento |
| enriched_text_hash | string | Hash del texto enriquecido |
| enrichment_language | string | Idioma usado con Comprehend |
| enriched_at_utc | timestamp | Fecha del análisis |
| pub_date | string (partición) | AAAA-MM-DD de `pub_ts_utc`, o `unknown` |

### `silver_news_entities`: una fila por entidad distinta detectada en cada noticia vigente

| Columna | Tipo | Descripción |
|---|---|---|
| news_id | string | Noticia |
| entity_type | string | PERSON, ORGANIZATION o LOCATION |
| entity_text | string | Texto de la entidad sin comillas ni signos en los extremos |
| entity_norm | string | `entity_text` en minúsculas y con espacios normalizados |
| mentions | int | Veces que se detectó en el texto |
| max_score | double | Confianza máxima devuelta por Comprehend |
| text_hash | string | Hash del texto analizado. Coincide con `content_hash` de la noticia |
| language | string | Idioma del análisis |
| pub_ts_utc | timestamp | Fecha de la noticia |
| pub_date | string (partición) | Igual a la de la noticia |

### `silver_news_key_phrases`: una fila por frase clave distinta detectada en cada noticia vigente

| Columna | Tipo | Descripción |
|---|---|---|
| news_id | string | Noticia |
| phrase | string | Frase sin artículo inicial (el, la, los, las, un, una...) |
| phrase_norm | string | `phrase` en minúsculas |
| mentions | int | Veces que se detectó |
| max_score | double | Confianza máxima |
| text_hash, language, pub_ts_utc, pub_date | | Igual que en entidades |

### Caché de Comprehend: `silver/_enrichment_cache/text_hash=<hash>.json`

Guarda el texto analizado, su hash, el idioma configurado, el idioma detectado y su puntaje, la fecha del análisis, el estado y la respuesta de Comprehend con sus puntuaciones de confianza.

### Cuarentena: `quarantine/silver/run_id=<lote>/rejected.jsonl`

Una línea por registro rechazado con `run_id`, `source_id`, `bronze_uri`, `item_index`, `reason` y `raw_item` (XML original del item).

## Gold

Todas las tablas están particionadas por `pub_date` y se recalculan desde la versión vigente de Silver. Las métricas cuentan noticias distintas. Las repeticiones de una entidad dentro de un mismo texto no inflan el indicador.

### `gold_news_daily`

Granularidad: una fila por fecha de publicación, medio y sección.

| Columna | Definición |
|---|---|
| medio, seccion | Dimensiones |
| news_count | Noticias vigentes distintas publicadas ese día en esa sección |

Una noticia que aparece en dos secciones cuenta una vez en cada sección. Por eso la suma de todas las secciones puede ser mayor que el total de noticias únicas.

### `gold_entity_daily`

Granularidad: una fila por fecha de publicación, tipo de entidad y entidad normalizada.

| Columna | Definición |
|---|---|
| entity_type, entity_norm | Dimensiones |
| entity_text | Forma de escritura más frecuente |
| news_count | Noticias distintas que mencionan la entidad (indicador de presencia) |
| mentions | Total de menciones detectadas (informativo) |

Como cada noticia tiene una sola fecha, sumar `news_count` entre fechas da el número de noticias distintas del periodo.

### `gold_key_phrase_section_daily`

Granularidad: una fila por fecha de publicación, sección y frase normalizada.

| Columna | Definición |
|---|---|
| seccion, phrase_norm | Dimensiones |
| phrase | Forma más frecuente |
| news_count | Noticias distintas de la sección que contienen la frase |

### Noticias sin fecha de publicación válida

No se rechazan, porque tienen título y enlace válidos. Quedan con `pub_ts_utc` nulo, `pub_date_status` en missing o invalid, y en la partición `pub_date=unknown`. En Gold se conservan en esa partición, de modo que los totales concilian con Silver. Las consultas por día las excluyen con `pub_date <> 'unknown'`.
