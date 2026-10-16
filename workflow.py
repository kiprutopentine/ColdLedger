"""Temporal workflow for ColdLedger.

The workflow body is deterministic: no direct IO, no clock reads except
workflow.now(), no randomness. It orchestrates the side-effecting activities
(ingest, check, alert) and owns the retry policy. A loop variant uses
workflow.sleep for a durable "monitor across the day" that survives restarts.
"""
from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from activities import (
        AlertInput,
        alert,
        check_breaches,
        ingest_batch,
    )

# Retry policy for activities. ConfigError (bad DSN, etc.) is non-retryable so a
# misconfiguration fails fast instead of looping; transient DB hiccups retry.
RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    maximum_interval=timedelta(seconds=10),
    maximum_attempts=5,
    non_retryable_error_types=["ConfigError"],
)


@workflow.defn
class ColdLedgerWorkflow:
    """One run: ingest a batch, compute the day's breaches, fire an idempotent
    alert per distinct excursion. Safe to re-run for the same day (0 new
    alerts)."""

    @workflow.run
    async def run(self, rows: list[dict], day: str | None = None) -> dict:
        # workflow.now() is the deterministic clock. If no day is passed, derive
        # it from workflow time so a replay picks the same day key.
        if day is None:
            day = workflow.now().date().isoformat()

        inserted = await workflow.execute_activity(
            ingest_batch, rows,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RETRY,
        )

        breaches = await workflow.execute_activity(
            check_breaches, day,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RETRY,
        )

        fired = 0
        for b in breaches:
            did_fire = await workflow.execute_activity(
                alert,
                AlertInput(
                    sensor_id=b["sensor_id"],
                    day=day,
                    start=b["start"],
                    end=b["end"],
                    minutes_over=b["minutes_over"],
                    max_temp=b["max_temp"],
                ),
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RETRY,
            )
            if did_fire:
                fired += 1

        return {
            "day": day,
            "inserted": inserted,
            "breaches": len(breaches),
            "alerts_fired": fired,
        }


@workflow.defn
class ColdLedgerMonitorLoop:
    """Durable monitor. Ingests the batch once, then wakes on an interval to
    re-check and alert. workflow.sleep is durable: if the worker dies during the
    sleep, Temporal resumes the timer on restart, so the monitor keeps its place
    across a crash. Bounded by `cycles` so the demo terminates; a real monitor
    would run continue-as-new indefinitely."""

    @workflow.run
    async def run(
        self,
        rows: list[dict],
        day: str,
        interval_seconds: int = 3600,
        cycles: int = 24,
    ) -> dict:
        inserted = await workflow.execute_activity(
            ingest_batch, rows,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RETRY,
        )
        total_fired = 0
        for _ in range(cycles):
            breaches = await workflow.execute_activity(
                check_breaches, day,
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RETRY,
            )
            for b in breaches:
                did_fire = await workflow.execute_activity(
                    alert,
                    AlertInput(
                        sensor_id=b["sensor_id"], day=day, start=b["start"],
                        end=b["end"], minutes_over=b["minutes_over"],
                        max_temp=b["max_temp"],
                    ),
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=RETRY,
                )
                if did_fire:
                    total_fired += 1
            await workflow.sleep(timedelta(seconds=interval_seconds))
        return {"day": day, "inserted": inserted, "alerts_fired": total_fired}
