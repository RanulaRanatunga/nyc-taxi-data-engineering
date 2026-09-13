CREATE TABLE IF NOT EXISTS dim_date (
    date_id       INT PRIMARY KEY,
    full_date     DATE NOT NULL UNIQUE,
    year          INT NOT NULL,
    quarter       INT NOT NULL,
    month         INT NOT NULL,
    month_name    VARCHAR(12) NOT NULL,
    day_of_month  INT NOT NULL,
    day_of_week   INT NOT NULL,
    day_name      VARCHAR(12) NOT NULL,
    week_of_year  INT NOT NULL,
    is_weekend    BOOLEAN NOT NULL,
    is_holiday    BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_location (
    location_id   INT PRIMARY KEY,
    borough       VARCHAR(50) NOT NULL,
    zone          VARCHAR(100) NOT NULL,
    service_zone  VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_payment_type (
    payment_type_id  INT PRIMARY KEY,
    payment_name     VARCHAR(50) NOT NULL,
    description      VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS dim_rate_code (
    rate_code_id      INT PRIMARY KEY,
    rate_description  VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_vendor (
    vendor_id    INT PRIMARY KEY,
    vendor_name  VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_taxi_trips (
    trip_id                BIGSERIAL PRIMARY KEY,
    vendor_id              INT NOT NULL REFERENCES dim_vendor (vendor_id),
    pickup_date_id         INT NOT NULL REFERENCES dim_date (date_id),
    dropoff_date_id        INT NOT NULL REFERENCES dim_date (date_id),
    pulocation_id          INT NOT NULL REFERENCES dim_location (location_id),
    dolocation_id          INT NOT NULL REFERENCES dim_location (location_id),
    rate_code_id           INT NOT NULL REFERENCES dim_rate_code (rate_code_id),
    payment_type_id        INT NOT NULL REFERENCES dim_payment_type (payment_type_id),
    pickup_datetime        TIMESTAMP NOT NULL,
    dropoff_datetime       TIMESTAMP NOT NULL,
    pickup_hour            SMALLINT NOT NULL,
    store_and_fwd_flag     CHAR(1) NOT NULL,
    passenger_count        SMALLINT,
    trip_distance          NUMERIC(10, 2) NOT NULL,
    fare_amount            NUMERIC(10, 2) NOT NULL,
    extra                  NUMERIC(10, 2) NOT NULL,
    mta_tax                NUMERIC(10, 2) NOT NULL,
    tip_amount             NUMERIC(10, 2) NOT NULL,
    tolls_amount           NUMERIC(10, 2) NOT NULL,
    improvement_surcharge  NUMERIC(10, 2) NOT NULL,
    congestion_surcharge   NUMERIC(10, 2) NOT NULL,
    airport_fee            NUMERIC(10, 2) NOT NULL,
    total_amount           NUMERIC(10, 2) NOT NULL,
    trip_duration_minutes  NUMERIC(10, 2) NOT NULL,
    fare_per_mile          NUMERIC(12, 4) NOT NULL,
    batch_id               VARCHAR(100) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fact_pickup_datetime ON fact_taxi_trips (pickup_datetime);
CREATE INDEX IF NOT EXISTS idx_fact_pickup_date_id  ON fact_taxi_trips (pickup_date_id);
CREATE INDEX IF NOT EXISTS idx_fact_pulocation      ON fact_taxi_trips (pulocation_id);
CREATE INDEX IF NOT EXISTS idx_fact_dolocation      ON fact_taxi_trips (dolocation_id);
CREATE INDEX IF NOT EXISTS idx_fact_payment_type    ON fact_taxi_trips (payment_type_id);

CREATE TABLE IF NOT EXISTS etl_audit_log (
    audit_id                SERIAL PRIMARY KEY,
    batch_id                VARCHAR(100) NOT NULL,
    flow_run_id             VARCHAR(64),
    source_file             VARCHAR(255) NOT NULL,
    stage                   VARCHAR(20) NOT NULL,
    status                  VARCHAR(10) NOT NULL,
    rows_extracted          BIGINT NOT NULL DEFAULT 0,
    rows_cleaned            BIGINT NOT NULL DEFAULT 0,
    rows_rejected           BIGINT NOT NULL DEFAULT 0,
    rows_loaded             BIGINT NOT NULL DEFAULT 0,
    started_at              TIMESTAMP NOT NULL,
    finished_at             TIMESTAMP,
    execution_duration_sec  NUMERIC(10, 2),
    error_message           TEXT,
    created_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
