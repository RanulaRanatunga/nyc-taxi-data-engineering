WITH per_day_hour AS (
    SELECT
        d.day_of_week,
        d.day_name,
        f.pickup_date_id,
        f.pickup_hour,
        COUNT(*) AS trips
    FROM fact_taxi_trips f
    JOIN dim_date d ON d.date_id = f.pickup_date_id
    GROUP BY d.day_of_week, d.day_name, f.pickup_date_id, f.pickup_hour
)
SELECT
    day_of_week,
    day_name,
    pickup_hour,
    ROUND(CAST(SUM(trips) * 1.0 / COUNT(DISTINCT pickup_date_id) AS NUMERIC(18, 4)), 1) AS avg_trips
FROM per_day_hour
GROUP BY day_of_week, day_name, pickup_hour
ORDER BY day_of_week, pickup_hour;
