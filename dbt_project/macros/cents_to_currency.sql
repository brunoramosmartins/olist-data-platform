{% macro cents_to_currency(cents_expr) -%}
( cast(({{ cents_expr }}) as double) / 100.0 )
{%- endmacro %}
