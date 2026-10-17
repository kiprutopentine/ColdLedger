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

# Strict calendar-day format: YYYY-MM-DD only. Anything else is a clean 400,
# not a 500 from datetime.fromisoformat choking on a datetime/timezone string
# or an out-of-range month/day. The regex pins the shape; strptime then
# confirms it is a real calendar date (rejects e.g. 2026-13-99).
_DAY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _valid_day(day: str) -> bool:
    if not _DAY_RE.match(day):
        return False
    try:
        datetime.strptime(day, "%Y-%m-%d")
    except ValueError:
        return False
    return True

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


@app.get("/api/timeline")
def timeline(request: Request, day: str = "2026-10-12") -> JSONResponse:
    ip = _client_ip(request)
    if _rate_limited(ip):
        return JSONResponse(
            status_code=429,
            content={"error": "rate limit exceeded; retry later"},
            headers={"Retry-After": str(int(_RATE_WINDOW))},
        )

    if not _valid_day(day):
        return JSONResponse(
            status_code=400,
            content={"error": "day must be a valid YYYY-MM-DD date"},
        )

    conn = store.connect()
    exs = [e.as_dict() for e in store.excursions(conn, day)]
    total_min = sum(e["minutes_over"] for e in exs)
    headline = (
        f"{len(exs)} excursion{'s' if len(exs) != 1 else ''} over "
        f"{THRESHOLD:g}C, {total_min} min total"
    )
    return JSONResponse(
        {
            "day": day,
            "threshold_c": THRESHOLD,
            "sustain_minutes": SUSTAIN,
            "headline": headline,
            "excursions": exs,
            "disclaimer": (
                "A logging and alert aid, not a certified compliance record."
            ),
        }
    )


@app.get("/")
def index() -> FileResponse:
    return FileResponse(os.path.join("static", "index.html"))


# Static assets (only index.html here, but keep the mount for future files).
app.mount("/static", StaticFiles(directory="static"), name="static")
