{% macro string_to_int(col) %}
    CASE
        WHEN {{col}} = 'nan' THEN NULL
        ELSE CAST(REPLACE({{col}}, '.0', '') AS int)
    END
{% endmacro %}
