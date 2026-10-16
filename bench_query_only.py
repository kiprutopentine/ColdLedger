"""Query-only Phase 3 scale timing against already-loaded history."""
from __future__ import annotations
import time
import store


def main() -> None:
    conn = store.connect()
    print(f"total rows: {store.row_count(conn):,}")

    t0 = time.perf_counter()
    store.refresh_aggregate(conn)
    print(f"aggregate refresh: {time.perf_counter()-t0:.2f}s")

    day = "2026-01-01"

    def bench(fn, n=5):
        best = None
        res = None
        for _ in range(n):
            t0 = time.perf_counter()
            res = fn(conn, day)
            dt = time.perf_counter() - t0
            best = dt if best is None else min(best, dt)
        return best, res

    naive_t, naive = bench(store.excursions_naive)
    heavy_t, heavy = bench(store.excursions)
    print(f"naive raw-scan excursions(day):      {naive_t*1000:8.2f} ms ({len(naive)} breaches)")
    print(f"continuous-aggregate excursions(day):{heavy_t*1000:8.2f} ms ({len(heavy)} breaches)")
    print(f"speedup (naive/heavy): {naive_t/heavy_t:.2f}x")

    with conn.cursor() as cur:
        t0 = time.perf_counter()
        cur.execute("SELECT count(*) FROM readings WHERE temp_c > %s", (store.THRESHOLD_C,))
        raw_scan = cur.fetchone()[0]
        raw_dt = time.perf_counter() - t0
        t0 = time.perf_counter()
        cur.execute("SELECT count(*) FROM readings_1min WHERE max_temp > %s", (store.THRESHOLD_C,))
        agg_scan = cur.fetchone()[0]
        agg_dt = time.perf_counter() - t0
    print(f"\nfull-history minutes-over count:")
    print(f"  raw readings scan: {raw_dt*1000:8.2f} ms ({raw_scan} rows)")
    print(f"  aggregate scan:    {agg_dt*1000:8.2f} ms ({agg_scan} rows)")
    print(f"  speedup (raw/agg): {raw_dt/agg_dt:.2f}x")


if __name__ == "__main__":
    main()
