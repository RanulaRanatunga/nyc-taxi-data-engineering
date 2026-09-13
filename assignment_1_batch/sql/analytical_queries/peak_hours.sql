WITH hourly AS (
    SELECT
        pickup_hour,
        COUNT(*)            AS trip_count,
        SUM(total_amount)   AS revenue,
        AVG(trip_distance)  AS avg_distance
    FROM fact_taxi_trips
    GROUP BY pickup_hour
)
SELECT
    pickup_hour,
    trip_count,
    RANK() OVER (ORDER BY trip_count DESC)                                    AS volume_rank,
    ROUND(CAST(trip_count * 100.0 / SUM(trip_count) OVER () AS NUMERIC(18, 4)), 2) AS pct_of_trips,
    ROUND(CAST(revenue AS NUMERIC(18, 4)), 2)                                 AS total_revenue,
    ROUND(CAST(avg_distance AS NUMERIC(18, 4)), 2)                            AS avg_distance_miles,
    CASE
        WHEN RANK() OVER (ORDER BY trip_count DESC) <= 3 THEN 'Peak'
        WHEN RANK() OVER (ORDER BY trip_count DESC) <= 12 THEN 'Moderate'
        ELSE 'Off-peak'
    END                                                                       AS demand_level
FROM hourly
ORDER BY pickup_hour;
