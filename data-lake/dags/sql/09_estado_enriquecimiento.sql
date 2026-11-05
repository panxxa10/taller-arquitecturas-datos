-- Estados de procesamiento de Comprehend sobre las noticias vigentes
SELECT enrichment_status, COUNT(*) AS noticias
FROM silver_news
GROUP BY enrichment_status
ORDER BY noticias DESC;
