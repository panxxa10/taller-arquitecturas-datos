-- Con filtro de partición: Athena solo lee la carpeta pub_date indicada
SELECT COUNT(*) AS noticias, COUNT(DISTINCT news_id) AS distintas, MAX(length(nlp_text)) AS max_len
FROM silver_news
WHERE pub_date = '2026-11-03';
