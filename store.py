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
HEAVY_SQL = """
WITH buckets AS (
    SELECT
        sensor_id,
        bucket,
        max_temp,
        (max_temp > %(threshold)s) AS over
    FROM readings_1min
    WHERE bucket >= %(start)s AND bucket < %(end)s
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


def excursions(conn: psycopg.Connection, day: str) -> list[Excursion]:
    """Heavy path. Read the continuous aggregate (per-minute time_bucket rollup)
    and compute sustained excursions entirely in SQL with a gaps-and-islands
    window query. The engine does the bucketing, the max-per-bucket reduction,
    the island grouping, and the duration math. Python only shapes the finished
    rows."""
    start, end = _day_bounds(day)
    with conn.cursor() as cur:
        cur.execute(
            HEAVY_SQL,
            {
                "threshold": THRESHOLD_C,
                "sustain": SUSTAIN_MIN,
                "start": start,
                "end": end,
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
