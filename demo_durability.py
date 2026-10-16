"""Phase 5 durability gate (runs with NO Temporal server).

Two proofs, both offline:

1. Idempotency: call the alert activity twice for the same excursions. The first
   pass fires one alert per distinct excursion; the second pass fires zero and
   leaves alerts.log unchanged. This is the property that makes a resumed or
   retried run safe.

2. Time-skipping workflow test: run ColdLedgerMonitorLoop under Temporal's
   time-skipping test environment so a monitor that sleeps an hour between
   checks completes 24 cycles in milliseconds. The workflow logic (including the
   durable workflow.sleep loop) is exercised end to end without a real cluster
   and without real wall-clock waiting.

Exit code 0 and a printed PASS mean the gate passed.
"""
from __future__ import annotations

import asyncio
import json
import os

import activities
from activities import AlertInput, alert


def _reset_alert_state() -> None:
    for p in (activities.ALERTS_LOG, activities.SEEN_KEYS):
        if os.path.exists(p):
            os.remove(p)


def _log_lines() -> int:
    if not os.path.exists(activities.ALERTS_LOG):
        return 0
    with open(activities.ALERTS_LOG) as f:
        return sum(1 for _ in f)


SAMPLE_BREACHES = [
    {"sensor_id": "fridge-01", "day": "2026-10-12",
     "start": "2026-10-12T10:04:00+00:00", "end": "2026-10-12T10:35:00+00:00",
     "minutes_over": 32, "max_temp": 9.404},
    {"sensor_id": "fridge-01", "day": "2026-10-12",
     "start": "2026-10-12T17:04:00+00:00", "end": "2026-10-12T17:14:00+00:00",
     "minutes_over": 11, "max_temp": 7.11},
]


async def _fire_all(breaches) -> int:
    fired = 0
    for b in breaches:
        did = await alert(AlertInput(**b))
        if did:
            fired += 1
    return fired


async def idempotency_proof() -> bool:
    _reset_alert_state()
    first = await _fire_all(SAMPLE_BREACHES)
    log_after_first = _log_lines()
    second = await _fire_all(SAMPLE_BREACHES)
    log_after_second = _log_lines()

    print("idempotency proof (no server, direct activity calls):")
    print(f"  first pass  fired : {first}  (expected {len(SAMPLE_BREACHES)})")
    print(f"  log lines after 1 : {log_after_first}")
    print(f"  second pass fired : {second}  (expected 0)")
    print(f"  log lines after 2 : {log_after_second}")
    ok = (
        first == len(SAMPLE_BREACHES)
        and second == 0
        and log_after_first == log_after_second == len(SAMPLE_BREACHES)
    )
    print(f"  -> {'PASS' if ok else 'FAIL'}\n")
    return ok


async def time_skipping_proof() -> bool:
    """Run the sleep-loop monitor under the time-skipping test server."""
    from temporalio.testing import WorkflowEnvironment
    from temporalio.worker import Worker
    from workflow import ColdLedgerMonitorLoop

    _reset_alert_state()
    rows = []
    with open("data/day.jsonl") as f:
        rows = [json.loads(line) for line in f if line.strip()]

    async with await WorkflowEnvironment.start_time_skipping() as env:
        async with Worker(
            env.client,
            task_queue="cold-ledger-test",
            workflows=[ColdLedgerMonitorLoop],
            activities=[activities.ingest_batch, activities.check_breaches,
                        activities.alert],
        ):
            # 24 hourly cycles. With a real clock this is a day; the test server
            # skips the sleeps so it finishes in milliseconds.
            result = await env.client.execute_workflow(
                ColdLedgerMonitorLoop.run,
                args=[rows, "2026-10-12", 3600, 24],
                id="coldledger-timeskip",
                task_queue="cold-ledger-test",
            )
    print("time-skipping monitor proof (24 hourly cycles, no wall-clock wait):")
    print(f"  result: {result}")
    # Two distinct excursions in the day, each should fire exactly once across
    # all 24 cycles thanks to the idempotent alert.
    ok = result.get("alerts_fired") == 2
    print(f"  alerts_fired == 2 (one per excursion, not per cycle): "
          f"{'PASS' if ok else 'FAIL'}\n")
    return ok


async def main() -> None:
    idem = await idempotency_proof()
    tskip = await time_skipping_proof()
    _reset_alert_state()
    if idem and tskip:
        print("PASS: durability gate (idempotency + time-skipping monitor)")
    else:
        print("FAIL: durability gate")
        raise SystemExit(1)


if __name__ == "__main__":
    asyncio.run(main())
