-- Pregunta 3: frases clave más frecuentes por sección (top 10 por sección, noticias distintas)
SELECT seccion, frase, noticias
FROM (
    SELECT seccion,
           max_by(phrase, news_count) AS frase,
           SUM(news_count) AS noticias,
           row_number() OVER (PARTITION BY seccion ORDER BY SUM(news_count) DESC, phrase_norm) AS rk
    FROM gold_key_phrase_section_daily
    GROUP BY seccion, phrase_norm
)
WHERE rk <= 10
ORDER BY seccion, noticias DESC;
