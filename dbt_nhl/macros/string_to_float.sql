{% macro string_to_float(col) %}
    CASE
        WHEN {{col}} = 'nan' THEN NULL
        ELSE CAST(FORMAT( {{col }}, 2) AS DECIMAL(10, 2))
    END
{% endmacro %}
