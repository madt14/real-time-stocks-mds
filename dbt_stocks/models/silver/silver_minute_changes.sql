SELECT
    symbol,
    DATE_TRUNC('MINUTE', market_timestamp) AS time_bucket,
    AVG(change_percent) AS change_percent
FROM {{ ref('silver_stock_quotes') }}
GROUP BY symbol, DATE_TRUNC('MINUTE', market_timestamp)