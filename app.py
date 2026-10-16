"""ColdLedger breach-timeline UI.

FastAPI app with a JSON timeline endpoint and a single static page. Serves the
day's excursions computed by the TimescaleDB continuous-aggregate path, plus a
headline ("2 excursions over 5C, 22 min total"). Bind to loopback only.
"""
from __future__ import annotations

import os

from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

import store

app = FastAPI(title="ColdLedger")

THRESHOLD = float(os.environ.get("BREACH_THRESHOLD_C", "5.0"))
SUSTAIN = int(os.environ.get("BREACH_SUSTAIN_MINUTES", "10"))


@app.get("/api/timeline")
def timeline(day: str = "2026-10-12") -> JSONResponse:
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
