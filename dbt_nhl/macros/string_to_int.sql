{% macro string_to_int(col) %}
    CAST(REPLACE( {{col}} , '.0', '') as int)
{% endmacro %}
