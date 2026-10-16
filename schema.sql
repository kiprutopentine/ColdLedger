-- ColdLedger TimescaleDB schema.
-- Readings hypertable + a continuous aggregate used by the heavy excursion path.

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Raw readings. (sensor_id, ts) is the natural key for an upsert so a replayed
-- or resumed ingest cannot create duplicate rows.
CREATE TABLE IF NOT EXISTS readings (
    sensor_id TEXT        NOT NULL,
    ts        TIMESTAMPTZ NOT NULL,
    temp_c    DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (sensor_id, ts)
);

SELECT create_hypertable('readings', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS readings_sensor_idx ON readings (sensor_id, ts DESC);

-- Continuous aggregate: one row per sensor per minute bucket with the bucket's
-- max temperature. This is the materialized rollup the heavy excursion query
-- reads instead of scanning raw rows. time_bucket does the bucketing inside the
-- time-series engine; the rollup is kept fresh by a refresh policy.
CREATE MATERIALIZED VIEW IF NOT EXISTS readings_1min
WITH (timescaledb.continuous) AS
SELECT
    sensor_id,
    time_bucket('1 minute', ts) AS bucket,
    max(temp_c) AS max_temp,
    avg(temp_c) AS avg_temp
FROM readings
GROUP BY sensor_id, time_bucket('1 minute', ts)
WITH NO DATA;

-- The demo refreshes the aggregate explicitly via refresh_continuous_aggregate()
-- in the ingest path, which is deterministic and avoids a background policy
-- fighting a large initial backfill. A production deployment would add:
--   SELECT add_continuous_aggregate_policy('readings_1min',
--       start_offset => INTERVAL '1 day', end_offset => INTERVAL '1 minute',
--       schedule_interval => INTERVAL '1 minute');
-- so new readings roll up automatically. See NOTES.md for why the demo keeps
-- refreshes explicit (a policy racing a backfill caused LockNotAvailable).
