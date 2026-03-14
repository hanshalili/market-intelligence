{% test assert_drawdown_non_positive(model, column_name) %}

SELECT
    date,
    symbol,
    {{ column_name }}
FROM {{ model }}
WHERE
    {{ column_name }} IS NOT NULL
    AND {{ column_name }} > 0.001

{% endtest %}
