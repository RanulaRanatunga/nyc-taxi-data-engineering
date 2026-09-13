SELECT
    p.payment_type_id,
    p.payment_name,
    COUNT(*)                                                                  AS total_trips,
    ROUND(CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS NUMERIC(18, 4)), 2) AS pct_of_trips,
    ROUND(CAST(SUM(f.fare_amount) AS NUMERIC(18, 4)), 2)                      AS total_fare,
    ROUND(CAST(SUM(f.tip_amount) AS NUMERIC(18, 4)), 2)                       AS total_tips,
    ROUND(CAST(SUM(f.total_amount) AS NUMERIC(18, 4)), 2)                     AS total_revenue,
    ROUND(CAST(SUM(f.tip_amount) * 100.0 / NULLIF(SUM(f.fare_amount), 0) AS NUMERIC(18, 4)), 2) AS tip_pct_of_fare
FROM fact_taxi_trips f
JOIN dim_payment_type p ON p.payment_type_id = f.payment_type_id
GROUP BY p.payment_type_id, p.payment_name
ORDER BY total_revenue DESC;
