SELECT
    symbol,
    ROUND(current_price, 2) AS current_price,
    ROUND(change_amount, 2) AS change_amount,
    ROUND(change_percent, 2) AS change_percent,
    ROUND(day_high, 2) AS day_high,
    ROUND(day_low, 2) AS day_low,
    ROUND(day_open, 2) AS day_open,
    ROUND(previous_close, 2) AS previous_close,
    market_timestamp,
    fetched_at
FROM {{ ref('bronze_stock_quotes') }}
WHERE current_price IS NOT NULL