-- Falla (devuelve filas) si el indicador del dashboard no coincide con la tabla de hechos.
select dashboard_total, fact_total
from (
    select
        (select sum(subtotal) from {{ ref('rpt_sales_dashboard') }}) as dashboard_total,
        (select sum(subtotal) from {{ ref('fct_sales') }}) as fact_total
) as totals
where abs(dashboard_total - fact_total) > 0.01
   or (select count(*) from {{ ref('rpt_sales_dashboard') }}) <> (select count(*) from {{ ref('fct_sales') }})
