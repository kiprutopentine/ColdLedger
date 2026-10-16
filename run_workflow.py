"""Start one ColdLedgerWorkflow run against the local Temporal server.

Loads the generated day and submits a single workflow execution, then waits for
the result and prints it. Used both for the normal happy path and (with the
worker killed mid-run) for the kill-and-resume demo.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys

from temporalio.client import Client

from workflow import ColdLedgerWorkflow

ADDRESS = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
TASK_QUEUE = os.environ.get("TEMPORAL_TASK_QUEUE", "cold-ledger")


async def main() -> None:
    wf_id = sys.argv[1] if len(sys.argv) > 1 else "coldledger-run"
    with open("data/day.jsonl") as f:
        rows = [json.loads(line) for line in f if line.strip()]

    client = await Client.connect(ADDRESS)
    handle = await client.start_workflow(
        ColdLedgerWorkflow.run,
        args=[rows, "2026-10-12"],
        id=wf_id,
        task_queue=TASK_QUEUE,
    )
    print(f"started workflow {wf_id}")
    result = await handle.result()
    print(f"result: {result}")


if __name__ == "__main__":
    asyncio.run(main())
