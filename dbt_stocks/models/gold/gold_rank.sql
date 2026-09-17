WITH bucketed AS (
    SELECT
        symbol,
        DATE_TRUNC('MINUTE', market_timestamp) AS time_bucket,
        AVG(change_percent) AS change_percent
    FROM {{ ref('silver_stock_quotes') }}
    GROUP BY symbol, DATE_TRUNC('MINUTE', market_timestamp)
)
SELECT
    symbol,
    time_bucket,
    change_percent,
    RANK() OVER (
        PARTITION BY time_bucket ORDER BY change_percent DESC
    ) AS rank_by_change
FROM bucketed