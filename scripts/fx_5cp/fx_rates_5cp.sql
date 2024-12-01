-- Drop the table if it exists
DROP TABLE IF EXISTS processed_fx_rates;

CREATE TABLE processed_fx_rates AS
WITH active_rates AS (
    SELECT
        ccy_couple,
        MAX(event_time) AS latest_event_time
    FROM raw_fx_rates
    WHERE event_time >= (
        -- Filter for events that occurred in the last 30 seconds
        (EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000) - 3000000
    )
    GROUP BY ccy_couple
),
yesterday_rates AS (
    SELECT DISTINCT
        ccy_couple,
        FIRST_VALUE(rate) OVER (
            PARTITION BY ccy_couple
            ORDER BY event_time DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS yesterday_rate
    FROM raw_fx_rates
    WHERE event_time <= (
        -- Filter for events that happened yesterday  at 5 PM
        EXTRACT(EPOCH FROM (CURRENT_DATE - INTERVAL '1 day') + INTERVAL '17 hours') * 1000
    )
)
SELECT
    a.ccy_couple,
    f.rate AS current_rate,
    ROUND(CAST((f.rate - y.yesterday_rate) / y.yesterday_rate * 100 AS NUMERIC), 3) || '%' AS change
FROM active_rates a
JOIN raw_fx_rates f
    ON a.ccy_couple = f.ccy_couple AND a.latest_event_time = f.event_time
JOIN yesterday_rates y
    ON a.ccy_couple = y.ccy_couple;

