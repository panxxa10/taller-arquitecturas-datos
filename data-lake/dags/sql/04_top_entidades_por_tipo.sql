-- Pregunta 2: personas, organizaciones y lugares que aparecen en más noticias (top 10 por tipo)
SELECT entity_type, entidad, noticias
FROM (
    SELECT entity_type,
           max_by(entity_text, news_count) AS entidad,
           SUM(news_count) AS noticias,
           row_number() OVER (PARTITION BY entity_type ORDER BY SUM(news_count) DESC, entity_norm) AS rk
    FROM gold_entity_daily
    GROUP BY entity_type, entity_norm
)
WHERE rk <= 10
ORDER BY entity_type, noticias DESC;
