-- Las diez organizaciones presentes en más noticias (noticias distintas, no menciones)
SELECT entity_norm,
       max_by(entity_text, news_count) AS organizacion,
       SUM(news_count) AS noticias,
       SUM(mentions) AS menciones
FROM gold_entity_daily
WHERE entity_type = 'ORGANIZATION'
GROUP BY entity_norm
ORDER BY noticias DESC, menciones DESC, entity_norm
LIMIT 10;
