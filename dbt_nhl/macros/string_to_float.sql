{% macro string_to_float(col) %}
    CASE
        WHEN {{col}} = 'nan' THEN NULL
        ELSE CAST(FORMAT( {{col }}, 3) AS DECIMAL(10, 3))
    END
{% endmacro %}
