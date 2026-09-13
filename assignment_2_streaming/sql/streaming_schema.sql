CREATE TABLE IF NOT EXISTS streaming_taxi_trips (
    trip_id           VARCHAR(64) PRIMARY KEY,
    vendor_id         INT,
    pickup_datetime   TIMESTAMP NOT NULL,
    dropoff_datetime  TIMESTAMP NOT NULL,
    pulocation_id     INT NOT NULL,
    dolocation_id     INT NOT NULL,
    passenger_count   INT,
    trip_distance     NUMERIC(10, 2) NOT NULL,
    fare_amount       NUMERIC(10, 2) NOT NULL,
    tip_amount        NUMERIC(10, 2) NOT NULL,
    total_amount      NUMERIC(10, 2) NOT NULL,
    payment_type      INT,
    event_status      VARCHAR(16) NOT NULL,
    produced_at       TIMESTAMP,
    ingested_at       TIMESTAMP NOT NULL,
    kafka_partition   INT,
    kafka_offset      BIGINT
);

CREATE INDEX IF NOT EXISTS idx_stream_trips_pickup ON streaming_taxi_trips (pickup_datetime);
CREATE INDEX IF NOT EXISTS idx_stream_trips_location_pickup ON streaming_taxi_trips (pulocation_id, pickup_datetime);

CREATE TABLE IF NOT EXISTS windowed_trip_aggregates (
    window_start           TIMESTAMP NOT NULL,
    window_end             TIMESTAMP NOT NULL,
    location_id            INT NOT NULL,
    trip_count             INT NOT NULL,
    passenger_count        INT NOT NULL,
    total_fare             NUMERIC(12, 2) NOT NULL,
    total_tips             NUMERIC(12, 2) NOT NULL,
    total_revenue          NUMERIC(12, 2) NOT NULL,
    total_distance         NUMERIC(12, 2) NOT NULL,
    avg_fare_per_mile      NUMERIC(12, 4),
    avg_trip_duration_min  NUMERIC(10, 2),
    is_final               BOOLEAN NOT NULL,
    update_count           INT NOT NULL DEFAULT 1,
    last_updated_at        TIMESTAMP NOT NULL,
    PRIMARY KEY (window_start, window_end, location_id)
);

CREATE INDEX IF NOT EXISTS idx_window_location_start ON windowed_trip_aggregates (location_id, window_start);
