-- Pregunta 4: cómo cambia la presencia de una entidad entre fechas (cambiar el valor de entity_norm)
SELECT pub_date,
       entity_type,
       entity_text,
       news_count AS noticias,
       news_count - lag(news_count) OVER (PARTITION BY entity_type ORDER BY pub_date) AS cambio_vs_dia_anterior
FROM gold_entity_daily
WHERE entity_norm = 'colombia'
  AND pub_date <> 'unknown'
ORDER BY entity_type, pub_date;
