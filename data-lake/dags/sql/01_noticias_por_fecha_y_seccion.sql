-- Noticias vigentes de una sección en un rango de fechas (filtra por partición pub_date)
SELECT n.pub_date, sec AS seccion, n.pub_ts_utc, n.title, n.link
FROM silver_news n
CROSS JOIN UNNEST(n.secciones) AS t(sec)
WHERE n.pub_date BETWEEN '2026-11-01' AND '2026-11-30'
  AND sec = 'politica'
ORDER BY n.pub_ts_utc DESC;
