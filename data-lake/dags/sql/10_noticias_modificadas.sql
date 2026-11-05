-- Noticias cuyo contenido cambió al menos una vez (version > 1)
SELECT news_id, version, title, first_seen_run_id, last_changed_run_id, enrichment_status, enriched_at_utc
FROM silver_news
WHERE version > 1
ORDER BY last_changed_run_id DESC;
