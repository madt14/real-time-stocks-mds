WITH bucketed AS (
    SELECT
        symbol,
        DATE_TRUNC('MINUTE', market_timestamp) AS time_bucket,
        AVG(change_percent) AS change_percent
    FROM {{ ref('silver_stock_quotes') }}
    GROUP BY symbol, DATE_TRUNC('MINUTE', market_timestamp)
)
SELECT
    a.symbol AS symbol_a,
    b.symbol AS symbol_b,
    ROUND(CORR(a.change_percent, b.change_percent), 3) AS correlation
FROM bucketed a
JOIN bucketed b ON a.time_bucket = b.time_bucket
GROUP BY a.symbol, b.symbol