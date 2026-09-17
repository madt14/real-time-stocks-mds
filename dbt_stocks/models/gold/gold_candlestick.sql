WITH daily AS (
    SELECT
        symbol,
        CAST(market_timestamp AS DATE) AS trade_date,
        market_timestamp,
        current_price,
        day_high,
        day_low
    FROM {{ ref('silver_stock_quotes') }}
),
candles AS (
    SELECT
        symbol,
        trade_date,
        FIRST_VALUE(current_price) OVER (
            PARTITION BY symbol, trade_date ORDER BY market_timestamp
        ) AS candle_open,
        LAST_VALUE(current_price) OVER (
            PARTITION BY symbol, trade_date ORDER BY market_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS candle_close,
        MAX(day_high) OVER (PARTITION BY symbol, trade_date) AS candle_high,
        MIN(day_low) OVER (PARTITION BY symbol, trade_date) AS candle_low
    FROM daily
)
SELECT DISTINCT
    symbol, trade_date, candle_open, candle_close, candle_high, candle_low
FROM candles