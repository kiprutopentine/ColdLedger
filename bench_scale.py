"""Phase 3 gate at realistic HACCP volume.

One day of one sensor is tiny, and at that size the continuous aggregate is not
faster than a raw scan. The real compliance use case is different: a food
business keeps months to years of readings across several fridges, and asks for
the breach timeline of ONE day out of that history, repeatedly.

This script loads a larger history (default 180 days x 4 sensors ~= 1.03M
readings), then times the naive raw-scan excursion query vs the
continuous-aggregate island query for a single day inside that history. The
honest question the Phase 3 gate asks: does the aggregate earn its place at the
volume the payer actually accumulates?

Writes nothing permanent beyond the readings it inserts into the same DB.
"""
from __future__ import annotations

import argparse
import time
from datetime import datetime, timedelta, timezone

import numpy as np

import store

SEED = 424242


def synth_history(days: int, sensors: int) -> list[dict]:
    rng = np.random.default_rng(SEED)
    start = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    rows: list[dict] = []
    for s in range(sensors):
        sid = f"bench-{s:02d}"
        for d in range(days):
            day0 = start + timedelta(days=d)
            base = 3.0 + 0.4 * np.sin(np.linspace(0, 6.28, 1440))
            noise = rng.normal(0.0, 0.15, 1440)
            temp = base + noise
            # one excursion on the target day (d==0) so there is a breach to find
            if d == 0:
                for i in range(600, 641):
                    temp[i] += 6.0 * np.sin((i - 600) / 40.0 * 3.14159)
            for i in range(1440):
                ts = day0 + timedelta(minutes=i)
                rows.append({"sensor_id": sid, "ts": ts.isoformat(),
                             "temp_c": round(float(temp[i]), 3)})
    return rows


def bench(fn, conn, day, n=5):
    best = None
    res = None
    for _ in range(n):
        t0 = time.perf_counter()
        res = fn(conn, day)
        dt = time.perf_counter() - t0
        best = dt if best is None else min(best, dt)
    return best, res


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=180)
    ap.add_argument("--sensors", type=int, default=4)
    args = ap.parse_args()

    conn = store.connect()
    store.apply_schema(conn)

    rows = synth_history(args.days, args.sensors)
    print(f"loading {len(rows):,} readings "
          f"({args.days} days x {args.sensors} sensors)...")
    t0 = time.perf_counter()
    inserted = store.insert_readings(conn, rows)
    print(f"inserted {inserted:,} new rows in {time.perf_counter()-t0:.1f}s")

    t0 = time.perf_counter()
    store.refresh_aggregate(conn)
    print(f"aggregate refreshed in {time.perf_counter()-t0:.1f}s")
    print(f"total rows in table: {store.row_count(conn):,}\n")

    day = "2026-01-01"
    naive_t, naive = bench(store.excursions_naive, conn, day)
    heavy_t, heavy = bench(store.excursions, conn, day)

    print(f"naive raw-scan excursions(day):     {naive_t*1000:8.2f} ms  "
          f"({len(naive)} breaches)")
    print(f"continuous-aggregate excursions(day):{heavy_t*1000:8.2f} ms  "
          f"({len(heavy)} breaches)")
    print(f"speedup (naive/heavy): {naive_t/heavy_t:.2f}x")

    # Also time the "scan the whole history" shape, which is what a naive
    # monthly/annual report would do and where the aggregate's pre-reduction
    # matters most.
    with conn.cursor() as cur:
        t0 = time.perf_counter()
        cur.execute("SELECT count(*) FROM readings WHERE temp_c > %s",
                    (store.THRESHOLD_C,))
        raw_scan = cur.fetchone()[0]
        raw_dt = time.perf_counter() - t0

        t0 = time.perf_counter()
        cur.execute("SELECT count(*) FROM readings_1min WHERE max_temp > %s",
                    (store.THRESHOLD_C,))
        agg_scan = cur.fetchone()[0]
        agg_dt = time.perf_counter() - t0
    print(f"\nfull-history 'minutes over threshold' count:")
    print(f"  raw readings scan:      {raw_dt*1000:8.2f} ms ({raw_scan} rows)")
    print(f"  aggregate scan:         {agg_dt*1000:8.2f} ms ({agg_scan} rows)")
    print(f"  speedup (raw/agg): {raw_dt/agg_dt:.2f}x")


if __name__ == "__main__":
    main()
