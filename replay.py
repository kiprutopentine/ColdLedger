"""Stream the generated day into the ingest path.

Default is a one-shot load: read data/day.jsonl, insert into TimescaleDB, refresh
the continuous aggregate. With --stream it inserts in minute-chunk batches with a
short pause so you can watch the timeline fill during a live demo.
"""
from __future__ import annotations

import argparse
import json
import time

import store


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", default="data/day.jsonl")
    ap.add_argument("--stream", action="store_true",
                    help="insert in batches with a pause (demo pacing)")
    ap.add_argument("--batch", type=int, default=120,
                    help="rows per batch in stream mode")
    ap.add_argument("--pause", type=float, default=0.2,
                    help="seconds between batches in stream mode")
    args = ap.parse_args()

    with open(args.file) as f:
        rows = [json.loads(line) for line in f if line.strip()]

    conn = store.connect()
    store.apply_schema(conn)

    if not args.stream:
        inserted = store.insert_readings(conn, rows)
        store.refresh_aggregate(conn)
        print(f"loaded {inserted} new rows; total {store.row_count(conn)}")
    else:
        total = 0
        for i in range(0, len(rows), args.batch):
            chunk = rows[i:i + args.batch]
            total += store.insert_readings(conn, chunk)
            store.refresh_aggregate(conn)
            print(f"  streamed {i + len(chunk)}/{len(rows)}")
            time.sleep(args.pause)
        print(f"streamed {total} rows")

    for e in store.excursions(conn, "2026-10-12"):
        print(f"  excursion {e.start} -> {e.end} {e.minutes_over} min "
              f"peak {e.max_temp}C")


if __name__ == "__main__":
    main()
