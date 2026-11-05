-- Particiones disponibles en Silver y Gold con su número de filas
SELECT 'silver_news' AS tabla, pub_date, COUNT(*) AS filas FROM silver_news GROUP BY pub_date
UNION ALL
SELECT 'gold_news_daily', pub_date, SUM(news_count) FROM gold_news_daily GROUP BY pub_date
ORDER BY tabla, pub_date;
