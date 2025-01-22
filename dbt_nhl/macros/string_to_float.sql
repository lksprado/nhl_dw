{% macro string_to_float(col) %}
    CAST(FORMAT( {{col }}, 2) AS DECIMAL(10, 2))
{% endmacro %}
