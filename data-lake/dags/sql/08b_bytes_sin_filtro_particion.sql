-- Sin filtro de partición: el mismo resultado filtrando por una columna normal obliga a leer todas las particiones
SELECT COUNT(*) AS noticias, COUNT(DISTINCT news_id) AS distintas, MAX(length(nlp_text)) AS max_len
FROM silver_news
WHERE date_format(pub_ts_utc, '%Y-%m-%d') = '2026-11-03';
