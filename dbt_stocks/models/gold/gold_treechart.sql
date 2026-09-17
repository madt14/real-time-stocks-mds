WITH stats AS (
    SELECT
        symbol,
        AVG(current_price) AS average_price,
        STDDEV_SAMP(current_price) AS volatility
    FROM {{ ref('silver_stock_quotes') }}
    GROUP BY symbol
)
SELECT
    symbol,
    ROUND(average_price, 2) AS average_price,
    ROUND(COALESCE(volatility, 0), 4) AS volatility,
    ROUND(COALESCE(volatility / NULLIF(average_price, 0), 0), 4) AS relative_volatility
FROM stats