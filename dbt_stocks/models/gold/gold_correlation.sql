SELECT
    a.symbol AS symbol_a,
    b.symbol AS symbol_b,
    ROUND(CORR(a.change_percent, b.change_percent), 3) AS correlation
FROM {{ ref('silver_minute_changes') }} a
JOIN {{ ref('silver_minute_changes') }} b ON a.time_bucket = b.time_bucket
GROUP BY a.symbol, b.symbol