{#
    Surrogate key = hash 16 hex dau cua md5(natural_key) ep sang bigint.
    On dinh giua cac lan chay du them/bot ban ghi (khac row_number(), se
    lech key khi co gia tri moi nho hon xen vao giua). Khong can dbt_utils.
#}
{% macro surrogate_key(column_name) -%}
abs(('x' || substr(md5({{ column_name }}::text), 1, 16))::bit(64)::bigint)
{%- endmacro %}
