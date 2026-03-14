{% test assert_positive_adjusted_close(model, column_name) %}

SELECT
    date,
    symbol,
    {{ column_name }}
FROM {{ model }}
WHERE
    {{ column_name }} IS NOT NULL
    AND {{ column_name }} <= 0

{% endtest %}
