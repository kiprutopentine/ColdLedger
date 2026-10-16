"""Generate one day of synthetic fridge/display-case temperature readings.

Deterministic (seeded). One reading per minute per sensor = 1440 points/sensor.
Baseline sits in a safe 2-4C band. Two excursions are injected on purpose so the
breach timeline has something to show:

  1. "door left open" ramp: temperature climbs well past the 5C threshold and
     stays there long enough to count as a sustained breach, then recovers.
  2. "defrost cycle" spike: a shorter, sharper rise over threshold.

A third, deliberately tiny single-minute blip is injected so the optional
blip-vs-breach gate (Phase 7) has a real false-positive to suppress. The
sustain-window logic already treats it as a non-breach, so threshold-only
output is unaffected.

Output: data/day.jsonl, one JSON object per line: {sensor_id, ts, temp_c}.
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timedelta, timezone

import numpy as np

READINGS_PER_DAY = 1440  # 1 per minute
SEED = 20261012  # challenge week, keeps generation reproducible


def _day_start() -> datetime:
    # Fixed UTC day so the demo is reproducible regardless of host clock.
    return datetime(2026, 10, 12, 0, 0, 0, tzinfo=timezone.utc)


def generate_sensor(sensor_id: str, rng: np.random.Generator) -> list[dict]:
    start = _day_start()
    # Baseline wandering around 3C with gentle noise.
    base = 3.0 + 0.4 * np.sin(np.linspace(0, 6.28, READINGS_PER_DAY))
    noise = rng.normal(0.0, 0.15, READINGS_PER_DAY)
    temp = base + noise

    # Excursion 1: door left open. Minutes 600-640 (10:00-10:40). A ramp up to
    # ~8.5C held above 5C, then recovery. ~30+ minutes sustained over 5C.
    for i in range(600, 641):
        progress = (i - 600) / 40.0
        # climb then hold then fall, peaking mid-window
        shape = np.sin(progress * 3.14159)
        temp[i] += 6.0 * shape

    # Excursion 2: defrost spike. Minutes 1020-1038 (17:00-17:18). Sharper, ~18
    # minutes over threshold peaking near 7.5C.
    for i in range(1020, 1039):
        progress = (i - 1020) / 18.0
        shape = np.sin(progress * 3.14159)
        temp[i] += 4.2 * shape

    # Lone sensor blip: a single minute spike to ~9C at minute 300. Too short to
    # be a sustained breach; it is the false positive the decider would suppress.
    temp[300] += 6.5

    rows = []
    for i in range(READINGS_PER_DAY):
        ts = start + timedelta(minutes=i)
        rows.append(
            {
                "sensor_id": sensor_id,
                "ts": ts.isoformat(),
                "temp_c": round(float(temp[i]), 3),
            }
        )
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate synthetic reading day.")
    ap.add_argument("--out", default="data/day.jsonl")
    ap.add_argument(
        "--sensors",
        default="fridge-01",
        help="comma-separated sensor ids",
    )
    args = ap.parse_args()

    rng = np.random.default_rng(SEED)
    sensors = [s.strip() for s in args.sensors.split(",") if s.strip()]

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    total = 0
    with open(args.out, "w") as f:
        for sid in sensors:
            for row in generate_sensor(sid, rng):
                f.write(json.dumps(row) + "\n")
                total += 1

    per_sensor = total // len(sensors)
    print(f"wrote {total} readings ({per_sensor}/sensor) to {args.out}")
    print(f"sensors: {', '.join(sensors)}")
    print("injected excursions: door-open ramp (~10:00), defrost spike (~17:00)")
    print("injected blip: single-minute spike (~05:00) -> not a sustained breach")


if __name__ == "__main__":
    main()
