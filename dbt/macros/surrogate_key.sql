{% macro surrogate_key(column_name) -%}
abs(('x' || substr(md5({{ column_name }}::text), 1, 16))::bit(64)::bigint)
{%- endmacro %}
