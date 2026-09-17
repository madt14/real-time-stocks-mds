SELECT
    symbol,
    current_price,
    change_amount,
    change_percent,
    fetched_at
FROM {{ ref('silver_stock_quotes') }}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY symbol ORDER BY fetched_at DESC
) = 1