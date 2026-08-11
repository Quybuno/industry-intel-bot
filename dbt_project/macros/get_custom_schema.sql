{# Ghi đè macro generate_schema_name chuẩn của dbt: mặc định dbt sinh schema dạng
   "<target_schema>_<custom_schema>", nhưng repo này chỉ có 3 schema cố định từ migration
   (bronze/silver/gold — xem alembic 0001) và mọi model/seed/snapshot của dbt đều khai
   +schema: gold một cách tường minh (dbt_project.yml). Ghi đè để dùng ĐÚNG schema đã khai,
   không nối chuỗi với target.schema (tránh sinh "gold_gold"). #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
