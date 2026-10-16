"""Temporal activities for ColdLedger. ALL side effects live here.

The workflow body stays deterministic; anything that touches the database, the
filesystem, or the clock is an activity. The alert activity is the durability
linchpin: it is idempotent, keyed by {sensor_id}:{day}, and writes with an
atomic os.replace so a retried or resumed run cannot double-fire an alert.
"""
from __future__ import annotations

import asyncio
import json
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone

from temporalio import activity

import store

ALERTS_LOG = os.environ.get("COLDLEDGER_ALERTS_LOG", "data/alerts.log")
SEEN_KEYS = os.environ.get("COLDLEDGER_SEEN_KEYS", "data/alert_keys.json")
# Optional artificial delay inside the alert activity, in seconds. Used only by
# the kill-and-resume demo so the worker can be killed mid-alert. Default 0.
ALERT_DELAY_S = float(os.environ.get("COLDLEDGER_ALERT_DELAY_S", "0"))


class ConfigError(Exception):
    """Raised for a misconfiguration that retrying cannot fix. The workflow
    marks this type non-retryable so a bad DSN fails fast instead of looping."""


def _ts(msg: str) -> None:
    """Print a wall-clock-stamped line to stdout for the kill-and-resume table."""
    now = datetime.now(timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    print(f"[{now}] {msg}", flush=True)


@dataclass
class AlertInput:
    sensor_id: str
    day: str
    start: str
    end: str
    minutes_over: int
    max_temp: float


def _load_seen() -> set[str]:
    if not os.path.exists(SEEN_KEYS):
        return set()
    try:
        with open(SEEN_KEYS) as f:
            return set(json.load(f))
    except (json.JSONDecodeError, OSError):
        return set()


def _atomic_write_json(path: str, data) -> None:
    """Write then os.replace so the file is never half-written. A crash leaves
    either the old file or the new one, never a torn mix."""
    d = os.path.dirname(path) or "."
    os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=d, prefix=".tmp-", suffix=".json")
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


def _append_alert_line(line: str) -> None:
    os.makedirs(os.path.dirname(ALERTS_LOG) or ".", exist_ok=True)
    with open(ALERTS_LOG, "a") as f:
        f.write(line + "\n")
        f.flush()
        os.fsync(f.fileno())


@activity.defn
async def ingest_batch(rows: list[dict]) -> int:
    """Write a batch of readings to TimescaleDB. Idempotent at the row level via
    ON CONFLICT DO NOTHING, so a replay or resume cannot duplicate readings."""
    try:
        conn = store.connect()
    except Exception as e:  # connection/config failure is not retryable-forever
        raise ConfigError(f"cannot connect to TimescaleDB: {e}") from e
    inserted = store.insert_readings(conn, rows)
    store.refresh_aggregate(conn)
    return inserted


@activity.defn
async def check_breaches(day: str) -> list[dict]:
    """Run the Phase 3 heavy excursion query and return the day's breaches."""
    conn = store.connect()
    return [e.as_dict() for e in store.excursions(conn, day)]


@activity.defn
async def alert(a: AlertInput) -> bool:
    """Idempotent breach alert. Key is {sensor_id}:{day}:{start-bucket}, so each
    distinct excursion fires exactly once (a day can hold more than one). Returns
    True if this call fired a new alert, False if the key was already seen.

    The seen-keys set is persisted with an atomic replace, and the key is
    recorded BEFORE the human-readable log line is appended, so a crash between
    the two still leaves the key marked seen (no double-fire on resume)."""
    key = f"{a.sensor_id}:{a.day}:{a.start}"
    seen = _load_seen()
    if key in seen:
        return False
    # Optional delay so the kill-and-resume demo can stop the worker here, after
    # the breach is known but before the alert is durably recorded. On restart
    # Temporal retries this activity from the top; the seen-key check above makes
    # the retry safe.
    if ALERT_DELAY_S > 0:
        _ts(f"alert START key={key} (delay {ALERT_DELAY_S}s before write)")
        await asyncio.sleep(ALERT_DELAY_S)
    seen.add(key)
    _atomic_write_json(SEEN_KEYS, sorted(seen))
    line = json.dumps(
        {
            "key": key,
            "sensor_id": a.sensor_id,
            "day": a.day,
            "start": a.start,
            "end": a.end,
            "minutes_over": a.minutes_over,
            "max_temp": a.max_temp,
        }
    )
    _append_alert_line(line)
    _ts(f"alert FIRED key={key}")
    return True
