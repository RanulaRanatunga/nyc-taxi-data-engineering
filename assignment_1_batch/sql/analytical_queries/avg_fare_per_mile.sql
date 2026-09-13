SELECT
    COUNT(*)                                                                  AS total_trips,
    ROUND(CAST(SUM(fare_amount) / NULLIF(SUM(trip_distance), 0) AS NUMERIC(18, 4)), 2) AS weighted_fare_per_mile,
    ROUND(CAST(AVG(fare_per_mile) AS NUMERIC(18, 4)), 2)                      AS simple_avg_fare_per_mile
FROM fact_taxi_trips;
