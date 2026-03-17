{% test expected_sum(model, column_name, expected_value) %}

SELECT
  ROUND(SUM({{ column_name }}), 2) AS actual_sum
FROM {{ model }}
HAVING ROUND(SUM({{ column_name }}), 2) != {{ expected_value }}

{% endtest %}