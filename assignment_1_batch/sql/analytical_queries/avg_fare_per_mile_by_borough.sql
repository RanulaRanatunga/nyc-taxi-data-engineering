SELECT
    loc.borough                                                               AS pickup_borough,
    COUNT(*)                                                                  AS total_trips,
    ROUND(CAST(SUM(f.trip_distance) AS NUMERIC(18, 4)), 2)                    AS total_miles,
    ROUND(CAST(SUM(f.fare_amount) AS NUMERIC(18, 4)), 2)                      AS total_fare,
    ROUND(CAST(SUM(f.fare_amount) / NULLIF(SUM(f.trip_distance), 0) AS NUMERIC(18, 4)), 2) AS weighted_fare_per_mile
FROM fact_taxi_trips f
JOIN dim_location loc ON loc.location_id = f.pulocation_id
GROUP BY loc.borough
ORDER BY total_trips DESC;
