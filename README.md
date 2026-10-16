# ColdLedger

A cold-chain temperature stream becomes a breach timeline and a durable alert
log. Replay a day of fridge readings, see every excursion over threshold with
its start, end, duration, and peak temperature, and get an alert that survives a
worker crash without firing twice or losing a reading.

ColdLedger is a logging and alert aid, not a certified compliance record.

## What it does

- Ingests a time-series of temperature readings into TimescaleDB as a hypertable.
- Computes sustained excursions with a continuous aggregate (`time_bucket`) and
  an engine-side gaps-and-islands query, not a raw row scan.
- Runs a durable Temporal workflow that ingests, checks, and alerts, with an
  idempotent alert keyed by `{sensor_id}:{day}:{excursion-start}`.
- Shows the day's breach timeline on a one-page UI.

## Prerequisites

- Docker (for TimescaleDB)
- Python 3.12 with venv
- Temporal CLI (`temporal server start-dev`) for the durable workflow. Not
  needed for the offline durability proof.

## Setup

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env            # defaults match docker-compose.yml

docker compose up -d            # TimescaleDB on localhost:5433
```

## Run the demo

```bash
# 1. Generate one day of readings with injected excursions.
python gen_readings.py                 # -> data/day.jsonl (1440 rows)

# 2. Load into TimescaleDB and print the breach timeline.
python replay.py

# 3. Breach timeline UI (loopback only).
bash serve.sh                          # http://127.0.0.1:8010
curl http://127.0.0.1:8010/api/timeline
```

## The two warrants

### Tiger Data (TimescaleDB): continuous aggregate + engine-side excursions

The breach computation reads a continuous aggregate and groups contiguous
over-threshold minute buckets into excursions in SQL:

```sql
CREATE MATERIALIZED VIEW readings_1min
WITH (timescaledb.continuous) AS
SELECT sensor_id, time_bucket('1 minute', ts) AS bucket,
       max(temp_c) AS max_temp, avg(temp_c) AS avg_temp
FROM readings
GROUP BY sensor_id, time_bucket('1 minute', ts);
```

Compared against a naive raw-row scan:

```bash
python compare_excursions.py                    # single clean day
python bench_scale.py --days 180 --sensors 4    # load ~1M rows
python bench_query_only.py                       # timings at scale
```

At one day the paths tie. At ~1M rows the aggregate query touches about 98%
fewer buffers (59 vs 5782) and returns 4 finished rows instead of 5760 raw rows,
a 1.31x single-day speedup, plus incremental refresh. Full numbers and query
plans are in NOTES.md.

### Temporal: durable, idempotent alerting

```bash
# Offline proof: idempotency + time-skipping monitor, no server needed.
python demo_durability.py            # prints PASS

# Live durable workflow:
temporal server start-dev            # in one shell
python worker.py                     # in another: registers on cold-ledger queue
python run_workflow.py my-run        # start one run

# Kill-and-resume proof against the live server:
bash kill_resume_demo.sh             # timestamped "late, not lost" table
```

If no Temporal server is running, `worker.py` prints how to start one and points
at `demo_durability.py`, which proves the idempotency property with direct
activity calls and no cluster.

## Layout

```
schema.sql            hypertable + continuous aggregate
store.py              ingest + naive and heavy excursion queries
gen_readings.py       synthetic day with injected excursions
replay.py             stream/load the day into the ingest path
activities.py         Temporal activities (all side effects; idempotent alert)
workflow.py           deterministic workflow + durable sleep-loop monitor
worker.py             Temporal worker on the cold-ledger queue
demo_durability.py    offline idempotency + time-skipping proof
kill_resume_demo.sh   live kill-and-resume proof
compare_excursions.py naive vs continuous-aggregate (single day)
bench_scale.py        load ~1M rows for the scale comparison
bench_query_only.py   query timings at scale
app.py + static/      breach timeline UI
docker-compose.yml    TimescaleDB
NOTES.md              verbatim proofs and build decisions
```

## Scope and honesty

- ColdLedger is a logging and alert aid. It is not a certified HACCP record.
- The blip-vs-breach model gate is deferred: the sustain window already rejects
  a single-minute spike, so a model adds nothing the code does not verify. See
  NOTES.md.
- The named food handler is a disclosed composite. See NOTES.md.
- Readings stay on the machine. Nothing is sent to a cloud service per reading.
