{{ config(materialized='table') }}

SELECT
    DATE_TRUNC('day', timestamp) AS day,
    COUNT(*) AS fraud_count,
    SUM(amount) AS fraud_amount
FROM
    processed_payments
WHERE
    is_fraud = true
GROUP BY
    1
