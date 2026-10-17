"""TimescaleDB access layer for ColdLedger.

Holds the schema bootstrap, bulk ingest, and the two excursion computations the
Phase 3 gate compares:

  excursions_naive(day)  -- the dumb baseline: pull every raw row for the day,
                            stitch runs over threshold in a Python loop.
  excursions(day)        -- the heavy path: read the continuous aggregate
                            (time_bucket rollup) and stitch sustained breaches
                            from the per-minute buckets using the time-series
                            engine for the heavy lifting.

Both return the same shape so the write-up can put them side by side:
    [{sensor_id, start, end, minutes_over, max_temp}]
"""
from __future__ import annotations

import os
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone

import psycopg

DEFAULT_DSN = os.environ.get(
    "COLDLEDGER_DSN",
    "postgresql://coldledger:coldledger@localhost:5433/coldledger",
)
THRESHOLD_C = float(os.environ.get("BREACH_THRESHOLD_C", "5.0"))
SUSTAIN_MIN = int(os.environ.get("BREACH_SUSTAIN_MINUTES", "10"))

# The three honest demo sensors. Everything else in the readings table (the
# bench-* sensors) is scale-only ballast loaded for the continuous-aggregate
# benchmark, not part of the demo story. The API separates the two so the UI
# can default to just these.
DEMO_SENSORS = ("fridge-01", "display-case-01", "walk-in-01")


@dataclass
class Excursion:
    sensor_id: str
    start: str          # ISO8601
    end: str            # ISO8601 (last minute still over threshold)
    minutes_over: int
    max_temp: float

    def as_dict(self) -> dict:
        return asdict(self)


def connect(dsn: str = DEFAULT_DSN) -> psycopg.Connection:
    return psycopg.connect(dsn, autocommit=True)


def apply_schema(conn: psycopg.Connection, schema_path: str = "schema.sql") -> None:
    with open(schema_path) as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)


def insert_readings(conn: psycopg.Connection, rows: list[dict]) -> int:
    """Idempotent bulk insert. COPY into a temp staging table, then insert with
    ON CONFLICT DO NOTHING so a replay or a resumed run that re-sends the same
    minute does not duplicate a reading."""
    if not rows:
        return 0
    # Run staging + upsert inside one transaction so the ON COMMIT DROP temp
    # table survives across the COPY and INSERT (the connection is autocommit,
    # so without an explicit transaction the temp table would vanish at once).
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                "CREATE TEMP TABLE _ingest (sensor_id TEXT, ts TIMESTAMPTZ, "
                "temp_c DOUBLE PRECISION) ON COMMIT DROP"
            )
            with cur.copy(
                "COPY _ingest (sensor_id, ts, temp_c) FROM STDIN"
            ) as copy:
                for r in rows:
                    copy.write_row((r["sensor_id"], r["ts"], r["temp_c"]))
            cur.execute(
                "INSERT INTO readings (sensor_id, ts, temp_c) "
                "SELECT sensor_id, ts, temp_c FROM _ingest "
                "ON CONFLICT (sensor_id, ts) DO NOTHING"
            )
            inserted = cur.rowcount
    return inserted


def refresh_aggregate(conn: psycopg.Connection) -> None:
    """Deterministic refresh of the continuous aggregate for the whole range.
    Called before computing excursions so the rollup reflects all ingested rows.
    CALL must run outside a transaction block; the connection is autocommit."""
    with conn.cursor() as cur:
        cur.execute(
            "CALL refresh_continuous_aggregate('readings_1min', NULL, NULL)"
        )


def row_count(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM readings")
        return cur.fetchone()[0]


def is_hypertable(conn: psycopg.Connection, name: str = "readings") -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM timescaledb_information.hypertables "
            "WHERE hypertable_name = %s",
            (name,),
        )
        return cur.fetchone()[0] == 1


def sensor_summary(conn: psycopg.Connection) -> list[dict]:
    """Per-sensor reading count and timestamp range, ordered by sensor_id.
    Each row is flagged `demo` so the API/UI can separate the three demo
    sensors from the bench-* scale ballast. One grouped scan of readings."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT sensor_id, count(*), min(ts), max(ts) "
            "FROM readings GROUP BY sensor_id ORDER BY sensor_id"
        )
        rows = cur.fetchall()
    out: list[dict] = []
    for sid, cnt, min_ts, max_ts in rows:
        out.append(
            {
                "sensor_id": sid,
                "count": int(cnt),
                "first_ts": min_ts.isoformat() if min_ts else None,
                "last_ts": max_ts.isoformat() if max_ts else None,
                "demo": sid in DEMO_SENSORS,
            }
        )
    return out


def distinct_sensor_count(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT count(DISTINCT sensor_id) FROM readings")
        return int(cur.fetchone()[0])


def date_range(conn: psycopg.Connection) -> tuple[str | None, str | None]:
    """Overall min/max reading timestamp across the whole table, ISO8601."""
    with conn.cursor() as cur:
        cur.execute("SELECT min(ts), max(ts) FROM readings")
        min_ts, max_ts = cur.fetchone()
    return (
        min_ts.isoformat() if min_ts else None,
        max_ts.isoformat() if max_ts else None,
    )


def sensor_ids_in_db(conn: psycopg.Connection) -> list[str]:
    """All distinct sensor_ids actually present in readings, sorted."""
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT sensor_id FROM readings ORDER BY sensor_id")
        return [r[0] for r in cur.fetchall()]


def available_days(
    conn: psycopg.Connection, sensor_id: str | None = None
) -> list[dict]:
    """List calendar days (UTC) that have readings, annotated with whether that
    day carried any sustained breach. When sensor_id is given, restrict to that
    sensor; otherwise cover the three demo sensors only (bench-* ballast is not
    part of the explorable demo surface).

    Breach presence is computed over the continuous aggregate with the same
    gaps-and-islands logic as excursions(): a day is FAIL if any sensor in scope
    has an island of at least SUSTAIN_MIN over-threshold minutes on that day.
    sensor_id is parameterized, never interpolated."""
    params: dict = {"threshold": THRESHOLD_C, "sustain": SUSTAIN_MIN}
    if sensor_id is not None:
        sensor_filter = "AND sensor_id = %(sensor_id)s"
        params["sensor_id"] = sensor_id
    else:
        sensor_filter = "AND sensor_id = ANY(%(demo)s)"
        params["demo"] = list(DEMO_SENSORS)

    sql = f"""
    WITH buckets AS (
        SELECT
            sensor_id,
            bucket,
            (bucket AT TIME ZONE 'UTC')::date AS day,
            (max_temp > %(threshold)s) AS over
        FROM readings_1min
        WHERE TRUE {sensor_filter}
    ),
    edges AS (
        SELECT
            sensor_id,
            day,
            bucket,
            over,
            CASE WHEN over <> COALESCE(
                    LAG(over) OVER (PARTITION BY sensor_id ORDER BY bucket),
                    FALSE)
                 THEN 1 ELSE 0 END AS is_edge
        FROM buckets
    ),
    grouped AS (
        SELECT
            sensor_id,
            day,
            over,
            SUM(is_edge) OVER (PARTITION BY sensor_id ORDER BY bucket) AS grp
        FROM edges
    ),
    islands AS (
        SELECT
            sensor_id,
            day,
            grp,
            count(*) AS minutes_over
        FROM grouped
        WHERE over
        GROUP BY sensor_id, day, grp
    ),
    day_breach AS (
        SELECT day, bool_or(minutes_over >= %(sustain)s) AS breached
        FROM islands
        GROUP BY day
    ),
    all_days AS (
        SELECT DISTINCT (bucket AT TIME ZONE 'UTC')::date AS day
        FROM readings_1min
        WHERE TRUE {sensor_filter}
    )
    SELECT
        a.day,
        COALESCE(b.breached, FALSE) AS breached
    FROM all_days a
    LEFT JOIN day_breach b ON a.day = b.day
    ORDER BY a.day
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    out: list[dict] = []
    for day, breached in rows:
        out.append(
            {
                "day": day.isoformat(),
                "breach": bool(breached),
                "status": "FAIL" if breached else "PASS",
            }
        )
    return out


def _day_bounds(day: str) -> tuple[datetime, datetime]:
    d = datetime.fromisoformat(day).replace(tzinfo=timezone.utc)
    start = d.replace(hour=0, minute=0, second=0, microsecond=0)
    return start, start + timedelta(days=1)


def _stitch(points: list[tuple[str, datetime, float]]) -> list[Excursion]:
    """Given per-minute (sensor_id, ts, max_temp) points ordered by time, stitch
    contiguous runs where temp > THRESHOLD_C and keep runs lasting at least
    SUSTAIN_MIN minutes. Shared by both paths so the comparison is apples to
    apples; the difference is where the per-minute points come from."""
    out: list[Excursion] = []
    run: list[tuple[datetime, float]] = []
    sensor = points[0][0] if points else ""
    for sid, ts, temp in points:
        if temp > THRESHOLD_C:
            run.append((ts, temp))
        else:
            if len(run) >= SUSTAIN_MIN:
                out.append(_run_to_excursion(sid, run))
            run = []
    if len(run) >= SUSTAIN_MIN:
        out.append(_run_to_excursion(sensor, run))
    return out


def _run_to_excursion(sid: str, run: list[tuple[datetime, float]]) -> Excursion:
    start = run[0][0]
    end = run[-1][0]
    minutes = len(run)
    peak = max(t for _, t in run)
    return Excursion(
        sensor_id=sid,
        start=start.isoformat(),
        end=end.isoformat(),
        minutes_over=minutes,
        max_temp=round(peak, 3),
    )


def excursions_naive(conn: psycopg.Connection, day: str) -> list[Excursion]:
    """Dumb baseline. Pull every raw reading for the day and stitch in Python.
    No time_bucket, no aggregate; a full row scan of the hypertable."""
    start, end = _day_bounds(day)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT sensor_id, ts, temp_c FROM readings "
            "WHERE ts >= %s AND ts < %s ORDER BY sensor_id, ts",
            (start, end),
        )
        rows = cur.fetchall()
    # Group by sensor, then stitch.
    by_sensor: dict[str, list[tuple[str, datetime, float]]] = {}
    for sid, ts, temp in rows:
        by_sensor.setdefault(sid, []).append((sid, ts, temp))
    out: list[Excursion] = []
    for sid in sorted(by_sensor):
        out.extend(_stitch(by_sensor[sid]))
    return out


# The heavy path does the whole excursion computation in SQL over the continuous
# aggregate: a gaps-and-islands query that groups contiguous over-threshold
# minute buckets into islands, then keeps islands lasting at least the sustain
# window. No Python row loop; the engine returns finished excursions.
#
# The buckets CTE carries an optional sensor filter. When %(sensor_id)s is NULL
# the filter is a no-op (all sensors); when it is a value the scan is limited to
# that one sensor. The value is always parameterized, never string-built.
HEAVY_SQL = """
WITH buckets AS (
    SELECT
        sensor_id,
        bucket,
        max_temp,
        (max_temp > %(threshold)s) AS over
    FROM readings_1min
    WHERE bucket >= %(start)s AND bucket < %(end)s
      AND (%(sensor_id)s::text IS NULL OR sensor_id = %(sensor_id)s::text)
),
edges AS (
    SELECT
        sensor_id,
        bucket,
        max_temp,
        over,
        -- mark a new island whenever the over/under state differs from the
        -- previous bucket for this sensor.
        CASE WHEN over <> COALESCE(
                LAG(over) OVER (PARTITION BY sensor_id ORDER BY bucket), FALSE)
             THEN 1 ELSE 0 END AS is_edge
    FROM buckets
),
grouped AS (
    SELECT
        sensor_id,
        bucket,
        max_temp,
        over,
        SUM(is_edge) OVER (PARTITION BY sensor_id ORDER BY bucket) AS grp
    FROM edges
),
islands AS (
    SELECT
        sensor_id,
        min(bucket) AS start_b,
        max(bucket) AS end_b,
        count(*) AS minutes_over,
        max(max_temp) AS max_temp
    FROM grouped
    WHERE over
    GROUP BY sensor_id, grp
)
SELECT sensor_id, start_b, end_b, minutes_over, max_temp
FROM islands
WHERE minutes_over >= %(sustain)s
ORDER BY sensor_id, start_b
"""


def excursions(
    conn: psycopg.Connection, day: str, sensor_id: str | None = None
) -> list[Excursion]:
    """Heavy path. Read the continuous aggregate (per-minute time_bucket rollup)
    and compute sustained excursions entirely in SQL with a gaps-and-islands
    window query. The engine does the bucketing, the max-per-bucket reduction,
    the island grouping, and the duration math. Python only shapes the finished
    rows.

    sensor_id is optional. When None (the default, and the historical behaviour)
    all sensors in the day are considered. When given, the scan is limited to
    that one sensor via a parameterized filter in the buckets CTE. The caller is
    responsible for validating sensor_id before it reaches here."""
    start, end = _day_bounds(day)
    with conn.cursor() as cur:
        cur.execute(
            HEAVY_SQL,
            {
                "threshold": THRESHOLD_C,
                "sustain": SUSTAIN_MIN,
                "start": start,
                "end": end,
                "sensor_id": sensor_id,
            },
        )
        rows = cur.fetchall()
    out: list[Excursion] = []
    for sid, start_b, end_b, minutes_over, max_temp in rows:
        out.append(
            Excursion(
                sensor_id=sid,
                start=start_b.isoformat(),
                end=end_b.isoformat(),
                minutes_over=int(minutes_over),
                max_temp=round(float(max_temp), 3),
            )
        )
    return out
