"""Phase 3 justify-or-drop gate: naive row-scan vs continuous-aggregate.

Loads the generated day into TimescaleDB, refreshes the continuous aggregate,
then runs both excursion computations and prints:
  - the excursions each path finds (they must agree on the real breaches),
  - wall-clock timing for each over repeated runs,
  - the EXPLAIN plan row counts so the "what the engine actually scans"
    difference is visible, not just asserted.

Run after `docker compose up -d` and `python gen_readings.py`.
"""
from __future__ import annotations

import json
import time

import store


def load_day(conn) -> int:
    with open("data/day.jsonl") as f:
        rows = [json.loads(line) for line in f if line.strip()]
    inserted = store.insert_readings(conn, rows)
    return inserted


def bench(fn, conn, day, n=5):
    best = None
    for _ in range(n):
        t0 = time.perf_counter()
        res = fn(conn, day)
        dt = time.perf_counter() - t0
        best = dt if best is None else min(best, dt)
    return best, res


def explain_rows(conn, sql, params) -> str:
    with conn.cursor() as cur:
        cur.execute("EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) " + sql, params)
        return "\n".join(r[0] for r in cur.fetchall())


def main() -> None:
    day = "2026-10-12"
    conn = store.connect()
    store.apply_schema(conn)
    print(f"hypertable? {store.is_hypertable(conn)}")
    inserted = load_day(conn)
    print(f"ingested {inserted} new rows; total now {store.row_count(conn)}")

    store.refresh_aggregate(conn)
    print("continuous aggregate refreshed\n")

    naive_t, naive = bench(store.excursions_naive, conn, day)
    heavy_t, heavy = bench(store.excursions, conn, day)

    print("=== excursions: NAIVE row-scan ===")
    for e in naive:
        print(f"  {e.sensor_id} {e.start} -> {e.end}  "
              f"{e.minutes_over} min over, peak {e.max_temp}C")
    print(f"  best of 5 runs: {naive_t*1000:.2f} ms\n")

    print("=== excursions: CONTINUOUS-AGGREGATE (time_bucket + SQL islands) ===")
    for e in heavy:
        print(f"  {e.sensor_id} {e.start} -> {e.end}  "
              f"{e.minutes_over} min over, peak {e.max_temp}C")
    print(f"  best of 5 runs: {heavy_t*1000:.2f} ms\n")

    agree = [e.as_dict() for e in naive] == [e.as_dict() for e in heavy]
    print(f"paths agree on breaches: {agree}")
    print(f"speedup (naive/heavy): {naive_t/heavy_t:.2f}x\n")

    # Show what each path actually scans.
    start, end = store._day_bounds(day)
    print("=== EXPLAIN: naive full raw-row scan ===")
    print(explain_rows(
        conn,
        "SELECT sensor_id, ts, temp_c FROM readings "
        "WHERE ts >= %s AND ts < %s ORDER BY sensor_id, ts",
        (start, end),
    ))
    print("\n=== EXPLAIN: heavy aggregate island query ===")
    print(explain_rows(
        conn,
        store.HEAVY_SQL,
        {"threshold": store.THRESHOLD_C, "sustain": store.SUSTAIN_MIN,
         "start": start, "end": end},
    ))


if __name__ == "__main__":
    main()
