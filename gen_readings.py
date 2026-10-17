"""Generate synthetic fridge/display-case temperature readings for Baridi.

Deterministic (seeded). One reading per minute per sensor = 1440 points/day.
Baseline sits in a safe 2-4C band. Excursions are injected on purpose so the
breach timeline has something honest to show.

Two generation modes:

  single-day (default, unchanged): writes the one featured demo day, fridge-01
  2026-10-12, exactly as the earlier phases produced it. Two excursions:

    1. "door left open" ramp: temperature climbs well past the 5C threshold and
       stays there long enough to count as a sustained breach, then recovers.
    2. "defrost cycle" spike: a shorter, sharper rise over threshold.

  A third, deliberately tiny single-minute blip is injected so the optional
  blip-vs-breach gate has a real false-positive to suppress. The sustain-window
  logic already treats it as a non-breach, so threshold-only output is
  unaffected.

  full (--full): writes ~30 days of minute-resolution readings for three demo
  sensors (fridge-01, display-case-01, walk-in-01), 2026-09-13 .. 2026-10-12
  inclusive, so the featured day is the last day. The featured fridge-01
  2026-10-12 day is drawn FIRST from its own dedicated rng(SEED) so it stays
  byte-for-byte identical no matter what else is generated. Every other
  (sensor, day) pair uses its own per-pair deterministic seed, so the whole set
  is reproducible and re-runnable and nothing can shift the featured draw.

Most days PASS on every sensor. A small, fixed set of days carry varied,
honest breaches: door-open ramps on fridge-01, sustained defrost spikes on
display-case-01, and one slow compressor-failure day on display-case-01.

Output: JSONL, one JSON object per line: {sensor_id, ts, temp_c}.
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timedelta, timezone

import numpy as np

READINGS_PER_DAY = 1440  # 1 per minute
SEED = 20261012  # challenge week, keeps featured-day generation reproducible

# Full-set configuration. The featured day is the LAST day in the range so the
# demo opens on the most recent reading.
FEATURED_SENSOR = "fridge-01"
FEATURED_DAY = datetime(2026, 10, 12, 0, 0, 0, tzinfo=timezone.utc)
DEMO_SENSORS = ["fridge-01", "display-case-01", "walk-in-01"]
FULL_DAYS = 30                      # 2026-09-13 .. 2026-10-12 inclusive
FULL_BASE_SEED = 90613              # base for per-(sensor, day) seeds

# Fixed, honest breach calendar. Keyed by (sensor_id, day-offset-from-start)
# where offset 0 is the first day (2026-09-13) and offset 29 is the featured
# day (2026-10-12). Most days are absent here, meaning they PASS. The featured
# fridge-01 day (offset 29) is handled by the preserved featured path and is
# NOT listed here.
#   "door"        -> fridge-01 door-open ramp, sustained breach
#   "defrost"     -> display-case-01 defrost spike that sustains past 10 min
#   "compressor"  -> display-case-01 slow compressor failure, long sustained
BREACH_CALENDAR = {
    ("fridge-01", 6): "door",
    ("fridge-01", 19): "door",
    ("display-case-01", 3): "defrost",
    ("display-case-01", 17): "defrost",
    ("display-case-01", 24): "compressor",
}


def _day_start() -> datetime:
    # Fixed UTC day so the demo is reproducible regardless of host clock.
    return FEATURED_DAY


def generate_sensor(sensor_id: str, rng: np.random.Generator) -> list[dict]:
    """Preserved featured-day generator. Produces exactly the fridge-01
    2026-10-12 shape: two excursions (10:04-10:35 and 17:04-17:14) plus the
    suppressed 05:00 blip. The rng must be a fresh default_rng(SEED) so the
    single rng.normal draw lands in the exact same position as before."""
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


def _pair_seed(sensor_index: int, day_offset: int) -> int:
    """Deterministic per-(sensor, day) seed. Independent of every other pair, so
    adding, removing, or reordering days cannot shift any other day's draw."""
    return FULL_BASE_SEED + sensor_index * 100000 + day_offset


def _baseline(sensor_id: str) -> np.ndarray:
    """Distinct, honest baseline band per sensor (no noise)."""
    x = np.linspace(0, 6.28, READINGS_PER_DAY)
    if sensor_id == "display-case-01":
        # Runs warmer: open front, closer to the 5C line but still passing.
        return 4.0 + 0.5 * np.sin(x)
    if sensor_id == "walk-in-01":
        # Very stable deep-cold room: low baseline, barely moves.
        return 2.5 + 0.15 * np.sin(x)
    # fridge-01 and any default: steady ~3C.
    return 3.0 + 0.4 * np.sin(x)


def _noise(sensor_id: str, rng: np.random.Generator) -> np.ndarray:
    """Per-sensor noise amplitude. walk-in-01 is the calmest."""
    if sensor_id == "walk-in-01":
        return rng.normal(0.0, 0.07, READINGS_PER_DAY)
    if sensor_id == "display-case-01":
        return rng.normal(0.0, 0.18, READINGS_PER_DAY)
    return rng.normal(0.0, 0.15, READINGS_PER_DAY)


def _apply_daily_defrost(temp: np.ndarray, breach: bool) -> None:
    """display-case-01 daily defrost cycle. Short, sharp spike near 13:00. On a
    normal day it is brief enough to NOT breach the 10-minute sustain window; on
    a chosen 'defrost' day it is widened and lifted so it sustains past 10 min."""
    if breach:
        # Wide, high defrost that stays over 5C for well past the sustain window.
        for i in range(780, 816):  # 13:00 .. 13:36
            progress = (i - 780) / 36.0
            temp[i] += 4.0 * np.sin(progress * 3.14159)
    else:
        # Brief defrost blip: a handful of minutes, recovers before 10 min.
        for i in range(780, 786):  # 13:00 .. 13:06
            progress = (i - 780) / 6.0
            temp[i] += 1.6 * np.sin(progress * 3.14159)


def _apply_door_ramp(temp: np.ndarray) -> None:
    """fridge-01 door-open ramp. Sustained breach, shape differs from the
    featured day so the non-featured breach days look distinct."""
    for i in range(540, 585):  # 09:00 .. 09:45
        progress = (i - 540) / 45.0
        temp[i] += 5.5 * np.sin(progress * 3.14159)


def _apply_compressor_failure(temp: np.ndarray) -> None:
    """display-case-01 slow compressor failure: climbs and stays high for a long
    sustained stretch (a couple of hours), the opposite of a quick spike."""
    for i in range(840, 1080):  # 14:00 .. 18:00
        progress = (i - 840) / 240.0
        # ramp up over ~40 min, hold high, then a slow partial recovery
        lift = 6.0 * min(1.0, progress * 6.0) * (1.0 - 0.3 * progress)
        temp[i] += lift


def generate_full_day(
    sensor_id: str, sensor_index: int, day_offset: int, day0: datetime
) -> list[dict]:
    """Generate one non-featured (sensor, day) at minute resolution from its own
    per-pair seed. Applies the fixed breach calendar; otherwise the day PASSES."""
    rng = np.random.default_rng(_pair_seed(sensor_index, day_offset))
    temp = _baseline(sensor_id) + _noise(sensor_id, rng)

    kind = BREACH_CALENDAR.get((sensor_id, day_offset))
    if sensor_id == "display-case-01":
        _apply_daily_defrost(temp, breach=(kind == "defrost"))
    if kind == "door":
        _apply_door_ramp(temp)
    elif kind == "compressor":
        _apply_compressor_failure(temp)

    rows = []
    for i in range(READINGS_PER_DAY):
        ts = day0 + timedelta(minutes=i)
        rows.append(
            {
                "sensor_id": sensor_id,
                "ts": ts.isoformat(),
                "temp_c": round(float(temp[i]), 3),
            }
        )
    return rows


def generate_full_set() -> list[dict]:
    """The full multi-sensor, multi-day set.

    The featured fridge-01 2026-10-12 day is drawn FIRST, from its own dedicated
    default_rng(SEED), so its single rng.normal draw is in the identical
    position it has always been and the day stays byte-for-byte. Every other
    (sensor, day) pair is then drawn from an independent per-pair rng, so no
    amount of added generation can move the featured draw."""
    start_day = FEATURED_DAY - timedelta(days=FULL_DAYS - 1)  # 2026-09-13
    featured_offset = FULL_DAYS - 1  # 29 -> 2026-10-12

    rows: list[dict] = []

    # 1) Featured day FIRST, preserved byte-for-byte.
    featured_rng = np.random.default_rng(SEED)
    rows.extend(generate_sensor(FEATURED_SENSOR, featured_rng))

    # 2) Everything else from independent per-pair rngs.
    for s_idx, sid in enumerate(DEMO_SENSORS):
        for d in range(FULL_DAYS):
            if sid == FEATURED_SENSOR and d == featured_offset:
                continue  # already emitted above, do not redraw
            day0 = start_day + timedelta(days=d)
            rows.extend(generate_full_day(sid, s_idx, d, day0))
    return rows


def _breach_summary() -> str:
    start_day = FEATURED_DAY - timedelta(days=FULL_DAYS - 1)
    lines = []
    for (sid, off), kind in sorted(BREACH_CALENDAR.items(), key=lambda kv: (kv[0][1], kv[0][0])):
        d = (start_day + timedelta(days=off)).date().isoformat()
        lines.append(f"  {d}  {sid}  {kind}")
    feat = FEATURED_DAY.date().isoformat()
    lines.append(f"  {feat}  {FEATURED_SENSOR}  featured (two excursions)")
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate synthetic reading data.")
    ap.add_argument("--out", default="data/day.jsonl")
    ap.add_argument(
        "--sensors",
        default="fridge-01",
        help="comma-separated sensor ids (single-day mode only)",
    )
    ap.add_argument(
        "--full",
        action="store_true",
        help=(
            "emit the full multi-sensor multi-day set "
            f"({len(DEMO_SENSORS)} sensors x {FULL_DAYS} days). Default output "
            "file becomes data/full.jsonl unless --out is given."
        ),
    )
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)

    if args.full:
        out = args.out
        if out == "data/day.jsonl":  # default was not overridden; use full name
            out = "data/full.jsonl"
        os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
        rows = generate_full_set()
        with open(out, "w") as f:
            for row in rows:
                f.write(json.dumps(row) + "\n")
        start_day = FEATURED_DAY - timedelta(days=FULL_DAYS - 1)
        print(f"wrote {len(rows):,} readings to {out}")
        print(
            f"sensors: {', '.join(DEMO_SENSORS)} | "
            f"{FULL_DAYS} days {start_day.date().isoformat()} .. "
            f"{FEATURED_DAY.date().isoformat()} inclusive"
        )
        print("breach calendar (all other days PASS):")
        print(_breach_summary())
        print(
            "featured fridge-01 2026-10-12 drawn first from rng(SEED): "
            "byte-for-byte preserved"
        )
        return

    # Single-day mode (unchanged default behaviour).
    rng = np.random.default_rng(SEED)
    sensors = [s.strip() for s in args.sensors.split(",") if s.strip()]
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
