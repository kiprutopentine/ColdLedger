"""ColdLedger Temporal worker.

Connects to the local Temporal dev server, registers the workflows and
activities on the cold-ledger task queue, and runs. If the server is not
reachable it prints exactly how to start it and points at the no-infra fallback
(demo_durability.py, which proves idempotency with direct activity calls and
needs no cluster).
"""
from __future__ import annotations

import asyncio
import os
import sys

from temporalio.client import Client
from temporalio.worker import Worker

from activities import alert, check_breaches, ingest_batch
from workflow import ColdLedgerMonitorLoop, ColdLedgerWorkflow

ADDRESS = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
TASK_QUEUE = os.environ.get("TEMPORAL_TASK_QUEUE", "cold-ledger")


async def main() -> None:
    try:
        client = await Client.connect(ADDRESS)
    except Exception as e:
        print(f"Could not reach a Temporal server at {ADDRESS}: {e}\n")
        print("Start the dev server in another shell:")
        print("    temporal server start-dev\n")
        print("No cluster handy? The durability and idempotency property is")
        print("provable without one:")
        print("    python demo_durability.py")
        sys.exit(1)

    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[ColdLedgerWorkflow, ColdLedgerMonitorLoop],
        activities=[ingest_batch, check_breaches, alert],
    )
    print(f"worker up: {ADDRESS} queue={TASK_QUEUE}")
    print("workflows: ColdLedgerWorkflow, ColdLedgerMonitorLoop")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
