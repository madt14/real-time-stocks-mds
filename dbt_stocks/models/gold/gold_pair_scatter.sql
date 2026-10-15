SELECT
    a.time_bucket,
    a.change_percent AS change_percent_a,
    b.change_percent AS change_percent_b
FROM {{ ref('silver_minute_changes') }} a
JOIN {{ ref('silver_minute_changes') }} b ON a.time_bucket = b.time_bucket
WHERE a.symbol = 'SYM_A' AND b.symbol = 'SYM_B'