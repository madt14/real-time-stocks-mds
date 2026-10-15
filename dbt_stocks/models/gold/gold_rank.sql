SELECT
    symbol,
    time_bucket,
    change_percent,
    RANK() OVER (
        PARTITION BY time_bucket ORDER BY change_percent DESC
    ) AS rank_by_change
FROM {{ ref('silver_minute_changes') }}