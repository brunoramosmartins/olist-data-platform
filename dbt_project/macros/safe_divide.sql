{% macro safe_divide(numerator, denominator) -%}
(
    case
        when ({{ denominator }}) is null then null
        when ({{ denominator }}) = 0 then null
        else cast(({{ numerator }}) as double) / cast(({{ denominator }}) as double)
    end
)
{%- endmacro %}
