{# 
    Override dbt default schema naming to prevent default prefix (e.g., 'main_staging' -> 'staging')
    Ensures clean schema separation in DuckDB Lakehouse.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
