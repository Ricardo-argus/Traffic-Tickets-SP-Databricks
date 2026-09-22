-- MACRO/SUM_COLUMN.SQL
{% macro sum_column(coluna) %}
    SUM({{ coluna }})
{% endmacro %}