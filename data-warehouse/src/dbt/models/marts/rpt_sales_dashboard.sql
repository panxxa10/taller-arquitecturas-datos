-- Tabla de consumo del dashboard de QuickSight: una fila por línea de venta con sus dimensiones ya resueltas.
-- Usa las versiones SCD2 vigentes al momento de la venta (product_sk y customer_sk vienen de fct_sales).
select
    fact.sale_sk,
    dates.date as sale_date,
    dates.year as sale_year,
    dates.month as sale_month,
    channel.sales_channel,
    product.name as product_name,
    product.category as product_category,
    geography.department,
    geography.city,
    fact.quantity,
    fact.unit_price,
    fact.subtotal
from {{ ref('fct_sales') }} as fact
inner join {{ ref('dim_date') }} as dates on dates.date_sk = fact.date_sk
left join {{ ref('dim_sales_channel') }} as channel on channel.sales_channel_sk = fact.sales_channel_sk
left join {{ ref('dim_product') }} as product on product.product_sk = fact.product_sk
left join {{ ref('dim_geography') }} as geography on geography.geography_sk = fact.geography_sk
