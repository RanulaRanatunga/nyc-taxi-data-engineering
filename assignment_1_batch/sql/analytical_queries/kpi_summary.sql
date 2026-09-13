SELECT
    COUNT(*)                                                         AS total_trips,
    ROUND(CAST(SUM(total_amount) AS NUMERIC(18, 4)), 2)              AS total_revenue,
    ROUND(CAST(SUM(trip_distance) AS NUMERIC(18, 4)), 2)             AS total_miles,
    ROUND(CAST(AVG(fare_amount) AS NUMERIC(18, 4)), 2)               AS avg_fare,
    MIN(pickup_datetime)                                             AS first_pickup,
    MAX(pickup_datetime)                                             AS last_pickup
FROM fact_taxi_trips;
