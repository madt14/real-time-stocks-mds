SELECT
    V:c::FLOAT AS current_price,
    V:d::FLOAT AS change_amount,
    V:dp::FLOAT AS change_percent,
    V:h::FLOAT AS day_high,
    V:l::FLOAT AS day_low,
    V:o::FLOAT AS day_open,
    V:pc::FLOAT AS previous_close,
    TO_TIMESTAMP_NTZ(V:t::NUMBER) AS market_timestamp,
    V:symbol::VARCHAR AS symbol,
    TO_TIMESTAMP_NTZ(V:fetched_at::NUMBER) AS fetched_at
FROM {{ source('raw', 'BRONZE_STOCK_QUOTES_RAW') }}
WHERE V:symbol IS NOT NULL