-- Conciliación: noticias por día y sección en Gold contra el conteo directo en Silver
WITH s AS (
    SELECT n.pub_date, sec AS seccion, COUNT(DISTINCT n.news_id) AS silver
    FROM silver_news n CROSS JOIN UNNEST(n.secciones) AS t(sec)
    GROUP BY 1, 2
),
g AS (
    SELECT pub_date, seccion, SUM(news_count) AS gold
    FROM gold_news_daily
    GROUP BY 1, 2
)
SELECT COALESCE(s.pub_date, g.pub_date) AS pub_date,
       COALESCE(s.seccion, g.seccion) AS seccion,
       s.silver, g.gold,
       COALESCE(s.silver, -1) = COALESCE(g.gold, -1) AS coincide
FROM s FULL OUTER JOIN g ON s.pub_date = g.pub_date AND s.seccion = g.seccion
ORDER BY 1, 2;
