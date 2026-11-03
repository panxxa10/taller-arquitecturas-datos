{# Funciones que difieren entre DuckDB (versión local) y Redshift (versión AWS). #}

{# Hora de negocio (America/Bogota) a partir de un timestamptz en UTC. #}
{% macro to_business_time(column) -%}
    {%- if target.type == 'redshift' -%}
        convert_timezone('UTC', '{{ var("business_timezone") }}', ({{ column }} at time zone 'UTC'))
    {%- else -%}
        timezone('{{ var("business_timezone") }}', {{ column }})
    {%- endif -%}
{%- endmacro %}

{# Día ISO de la semana: 1 = lunes ... 7 = domingo. Redshift no tiene isodow: extract(dow) da 0 = domingo ... 6 = sábado. #}
{% macro iso_day_of_week(date_column) -%}
    {%- if target.type == 'redshift' -%}
        case when extract(dow from {{ date_column }}) = 0 then 7 else extract(dow from {{ date_column }}) end
    {%- else -%}
        extract(isodow from {{ date_column }})
    {%- endif -%}
{%- endmacro %}

{# a IS DISTINCT FROM b, escrito sin depender de que el motor lo soporte. #}
{% macro is_distinct_from(left, right) -%}
    ({{ left }} <> {{ right }} or ({{ left }} is null and {{ right }} is not null) or ({{ left }} is not null and {{ right }} is null))
{%- endmacro %}

{# Texto de una ruta dentro de una columna JSON guardada como texto; nulo si falta la ruta o el JSON no es válido. #}
{% macro json_text(column, path) -%}
    {%- if target.type == 'redshift' -%}
        nullif(json_extract_path_text({{ column }}, {% for step in path %}'{{ step }}', {% endfor %}true), '')
    {%- else -%}
        json_extract_string({{ column }}, '$.{{ path | join(".") }}')
    {%- endif -%}
{%- endmacro %}

{# Días entre dos fechas. #}
{% macro days_between(start_date, end_date) -%}
    {%- if target.type == 'redshift' -%}
        datediff(day, {{ start_date }}, {{ end_date }})
    {%- else -%}
        datediff('day', {{ start_date }}, {{ end_date }})
    {%- endif -%}
{%- endmacro %}
