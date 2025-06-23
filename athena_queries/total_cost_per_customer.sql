SELECT
    customer_id,
    SUM(total_cost) AS total_cost_usd
FROM
    fact_transacciones_energia
WHERE
    YEAR(CAST(transaction_date AS DATE)) = 2025
GROUP BY
    customer_id
ORDER BY
    total_cost_usd DESC
LIMIT 10;