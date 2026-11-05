-- Pregunta 1: noticias publicadas por día, medio y sección (Gold)
SELECT pub_date, medio, seccion, news_count
FROM gold_news_daily
WHERE pub_date <> 'unknown'
ORDER BY pub_date, medio, seccion;
