"""Baridi breach-timeline UI.

FastAPI app with a JSON timeline endpoint and a single static page. Serves the
day's excursions computed by the TimescaleDB continuous-aggregate path, plus a
headline ("2 excursions over 5C, 22 min total"). Bind to loopback only.
"""
from __future__ import annotations

import os
import re
import threading
import time
from collections import defaultdict, deque
from datetime import datetime

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

import store

app = FastAPI(title="Baridi")

THRESHOLD = float(os.environ.get("BREACH_THRESHOLD_C", "5.0"))
SUSTAIN = int(os.environ.get("BREACH_SUSTAIN_MINUTES", "10"))

# The featured day's excursion query is the query the scale stat times live.
FEATURED_DAY = "2026-10-12"

# Strict calendar-day format: YYYY-MM-DD only. Anything else is a clean 400,
# not a 500 from datetime.fromisoformat choking on a datetime/timezone string
# or an out-of-range month/day. The regex pins the shape; strptime then
# confirms it is a real calendar date (rejects e.g. 2026-13-99).
_DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# Strict sensor-id shape: lowercase letters, digits, and hyphens only, 1..40
# chars. This rejects path traversal ("../bad"), SQL metacharacters, spaces,
# and anything else before the value is ever handed to the DB layer. The value
# is still passed to psycopg as a bound parameter, never interpolated; this
# allowlist is defence in depth and the source of the clean 400.
_SENSOR_RE = re.compile(r"^[a-z0-9-]{1,40}$")


def _valid_day(day: str) -> bool:
    if not _DAY_RE.match(day):
        return False
    try:
        datetime.strptime(day, "%Y-%m-%d")
    except ValueError:
        return False
    return True


def _valid_sensor(sensor_id: str) -> bool:
    return bool(_SENSOR_RE.match(sensor_id))

# Per-IP in-process sliding-window rate limit for /api/timeline.
# Best-effort, single-worker (see the uvicorn unit: --workers 1): keep a deque
# of recent request timestamps per client IP, drop anything older than the
# window, and reject with 429 once the window is full.
_RATE_MAX = int(os.environ.get("RATE_LIMIT_MAX", "30"))        # requests
_RATE_WINDOW = float(os.environ.get("RATE_LIMIT_WINDOW", "60"))  # seconds
_rate_hits: dict[str, deque[float]] = defaultdict(deque)
_rate_lock = threading.Lock()


def _client_ip(request: Request) -> str:
    """Resolve the caller's IP. Caddy terminates TLS and proxies to loopback,
    setting X-Forwarded-For; trust its first hop. Fall back to the socket peer
    when the header is absent (direct loopback call during testing)."""
    fwd = request.headers.get("x-forwarded-for")
    if fwd:
        first = fwd.split(",")[0].strip()
        if first:
            return first
    client = request.client
    return client.host if client else "unknown"


def _rate_limited(ip: str) -> bool:
    now = time.monotonic()
    cutoff = now - _RATE_WINDOW
    with _rate_lock:
        hits = _rate_hits[ip]
        while hits and hits[0] < cutoff:
            hits.popleft()
        if len(hits) >= _RATE_MAX:
            return True
        hits.append(now)
        return False


def _rate_limit_response() -> JSONResponse:
    return JSONResponse(
        status_code=429,
        content={"error": "rate limit exceeded; retry later"},
        headers={"Retry-After": str(int(_RATE_WINDOW))},
    )


def _db_error_response() -> JSONResponse:
    """Generic 500 for an unexpected DB/runtime failure. Never leaks a stack
    trace or driver message to the caller; the detail stays in the server log."""
    return JSONResponse(
        status_code=500,
        content={"error": "internal error computing result"},
    )


@app.get("/api/timeline")
def timeline(
    request: Request, day: str = "2026-10-12", sensor_id: str | None = None
) -> JSONResponse:
    ip = _client_ip(request)
    if _rate_limited(ip):
        return _rate_limit_response()

    # Validate every input BEFORE any DB call so bad input is always a clean
    # 400, never a 500.
    if not _valid_day(day):
        return JSONResponse(
            status_code=400,
            content={"error": "day must be a valid YYYY-MM-DD date"},
        )
    if sensor_id is not None and not _valid_sensor(sensor_id):
        return JSONResponse(
            status_code=400,
            content={"error": "sensor_id must match ^[a-z0-9-]{1,40}$"},
        )

    try:
        conn = store.connect()
        exs = [e.as_dict() for e in store.excursions(conn, day, sensor_id)]
    except Exception:
        return _db_error_response()

    total_min = sum(e["minutes_over"] for e in exs)
    headline = (
        f"{len(exs)} excursion{'s' if len(exs) != 1 else ''} over "
        f"{THRESHOLD:g}C, {total_min} min total"
    )
    body = {
        "day": day,
        "threshold_c": THRESHOLD,
        "sustain_minutes": SUSTAIN,
        "headline": headline,
        "excursions": exs,
        "disclaimer": (
            "A logging and alert aid, not a certified compliance record."
        ),
    }
    # Only surface the sensor_id key when the caller supplied one, so the
    # default (no sensor_id) response shape is byte-for-byte unchanged.
    if sensor_id is not None:
        body["sensor_id"] = sensor_id
    return JSONResponse(body)


@app.get("/api/sensors")
def sensors(request: Request) -> JSONResponse:
    """List every sensor in the DB with its reading count and timestamp range,
    flagged demo vs scale ballast. Also returns the demo sensor ids alone so a
    UI can default to the three honest sensors and hide the bench-* ballast."""
    ip = _client_ip(request)
    if _rate_limited(ip):
        return _rate_limit_response()
    try:
        conn = store.connect()
        all_sensors = store.sensor_summary(conn)
    except Exception:
        return _db_error_response()

    demo = [s for s in all_sensors if s["demo"]]
    return JSONResponse(
        {
            "sensors": all_sensors,
            "demo_sensors": [s["sensor_id"] for s in demo],
            "disclaimer": (
                "Synthetic sensor data. bench-* sensors are scale-test "
                "ballast, not part of the demo."
            ),
        }
    )


@app.get("/api/days")
def days(request: Request, sensor_id: str | None = None) -> JSONResponse:
    """List available calendar days annotated PASS/FAIL (whether any sustained
    breach occurred). With sensor_id, restrict to that sensor; without it, cover
    the three demo sensors. Expensive aggregate query, so rate limited."""
    ip = _client_ip(request)
    if _rate_limited(ip):
        return _rate_limit_response()

    if sensor_id is not None and not _valid_sensor(sensor_id):
        return JSONResponse(
            status_code=400,
            content={"error": "sensor_id must match ^[a-z0-9-]{1,40}$"},
        )

    try:
        conn = store.connect()
        day_rows = store.available_days(conn, sensor_id)
    except Exception:
        return _db_error_response()

    body: dict = {
        "days": day_rows,
        "disclaimer": (
            "Synthetic sensor data. PASS/FAIL reflects the 5C threshold over a "
            "10-minute sustain window."
        ),
    }
    if sensor_id is not None:
        body["sensor_id"] = sensor_id
    return JSONResponse(body)


@app.get("/api/scale")
def scale(request: Request) -> JSONResponse:
    """Real measured scale stats: total readings, distinct sensor count, overall
    date range, and a LIVE-measured query time (ms) for the featured day's
    excursion query. The ms is measured here with perf_counter, best of a few
    runs (like bench_query_only.py), never a hardcoded constant. Rate limited."""
    ip = _client_ip(request)
    if _rate_limited(ip):
        return _rate_limit_response()

    try:
        conn = store.connect()
        total = store.row_count(conn)
        n_sensors = store.distinct_sensor_count(conn)
        first_ts, last_ts = store.date_range(conn)
        # Time the heavy excursion query for the featured day, best of 5 runs.
        best_ms: float | None = None
        for _ in range(5):
            t0 = time.perf_counter()
            store.excursions(conn, FEATURED_DAY)
            elapsed_ms = (time.perf_counter() - t0) * 1000.0
            if best_ms is None or elapsed_ms < best_ms:
                best_ms = elapsed_ms
    except Exception:
        return _db_error_response()

    return JSONResponse(
        {
            "total_readings": total,
            "sensor_count": n_sensors,
            "first_ts": first_ts,
            "last_ts": last_ts,
            "featured_day": FEATURED_DAY,
            "featured_query_ms": round(best_ms, 3) if best_ms is not None else None,
            "query_runs": 5,
            "disclaimer": (
                "Synthetic sensor data. featured_query_ms is measured live on "
                "this request, not a fixed number."
            ),
        }
    )


@app.get("/")
def home() -> FileResponse:
    return FileResponse(os.path.join("static", "home.html"))


@app.get("/dashboard")
def dashboard() -> FileResponse:
    return FileResponse(os.path.join("static", "index.html"))


# Static assets (only index.html here, but keep the mount for future files).
app.mount("/static", StaticFiles(directory="static"), name="static")
