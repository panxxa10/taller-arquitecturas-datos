-- SQL que valida el indicador "Ventas totales" del dashboard (ejecutar en Redshift, esquema marts).
SELECT SUM(subtotal) AS ventas_totales FROM marts.rpt_sales_dashboard;   -- valor del KPI de QuickSight
SELECT SUM(subtotal) AS control_hechos   FROM marts.fct_sales;           -- debe ser idéntico
-- Detalle del gráfico (ventas por categoría):
SELECT COALESCE(product_category, '(sin categoría)') AS categoria, SUM(subtotal) AS ventas
FROM marts.rpt_sales_dashboard GROUP BY 1 ORDER BY 2 DESC;
