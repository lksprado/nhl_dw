{% macro string_to_min(col) %}
    (CAST(split_part({{ col }}, ':', 1) AS INTEGER) *60 + CAST(split_part({{ col }}, ':', 2) AS INTEGER))/60
{% endmacro %}
