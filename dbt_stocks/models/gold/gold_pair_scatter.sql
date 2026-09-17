WITH bucketed AS (
    SELECT 
        symbol,
        DATE_TRUNC('MINUTE', market_timestamp) AS time_bucket,
        AVG(change_percent) AS change_percent
    FROM {{ ref('silver_stock_quotes') }}
    GROUP BY symbol, DATE_TRUNC('MINUTE', market_timestamp) 
) 
SELECT
    a.time_bucket,
    a.change_percent AS change_percent_a,
    b.change_percent AS change_percent_b
FROM bucketed a 
JOIN bucketed b ON a.time_bucket = b.time_bucket 
WHERE a.symbol = 'GOOGL' AND b.symbol = 'AMZN'