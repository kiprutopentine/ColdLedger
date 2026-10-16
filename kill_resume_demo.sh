#!/usr/bin/env bash
# Kill-and-resume durability demo against a live Temporal dev server.
#
# Timeline: worker A starts with a long alert delay, the workflow starts, we let
# the first alert begin, then kill worker A mid-alert. No worker is running when
# the alert would have fired. We restart worker B (no delay); Temporal retries
# the pending activity and it completes. The alert fires late, is not lost, and
# does not double-fire.
set -u
cd ~/build/coldledger
. .venv/bin/activate

ts() { date -u +"%H:%M:%S.%3N"; }
WF_ID="coldledger-killresume-$(date +%s)"

# Clean alert state so we count this run's alerts only.
rm -f data/alerts.log data/alert_keys.json
echo "=== KILL-AND-RESUME DEMO ($WF_ID) ==="

# Worker A: slow alert so we can kill it mid-write.
COLDLEDGER_ALERT_DELAY_S=25 python worker.py > data/workerA.log 2>&1 &
WA=$!
echo "[$(ts)] worker A started (pid $WA, 25s alert delay)"
sleep 4

# Start the workflow (returns when workflow completes; run in background so we
# can act while it is mid-flight).
python run_workflow.py "$WF_ID" > data/run.log 2>&1 &
RUN=$!
echo "[$(ts)] workflow $WF_ID submitted"

# Wait until worker A logs the first alert START (breach known, not yet written).
for i in $(seq 1 40); do
  if grep -q "alert START" data/workerA.log 2>/dev/null; then
    echo "[$(ts)] first alert START observed on worker A:"
    grep "alert START" data/workerA.log | head -1 | sed 's/^/    /'
    break
  fi
  sleep 1
done

# Kill worker A mid-alert. The 25s sleep is still running, so the alert has NOT
# been durably written yet.
kill -9 "$WA" 2>/dev/null
echo "[$(ts)] worker A KILLED (kill -9) mid-alert; no worker is running now"
echo "[$(ts)] alerts.log line count while down: $(wc -l < data/alerts.log 2>/dev/null || echo 0)"
sleep 5

# Restart worker B (no delay). Temporal will redeliver the pending activity.
COLDLEDGER_ALERT_DELAY_S=0 python worker.py > data/workerB.log 2>&1 &
WB=$!
echo "[$(ts)] worker B restarted (pid $WB, no delay); Temporal should retry the pending activity"

# Wait for the workflow to finish.
wait "$RUN" 2>/dev/null
echo "[$(ts)] workflow finished"
echo "---- run.log ----"
cat data/run.log | sed 's/^/    /'

echo "---- worker B alert lines ----"
grep -E "alert (START|FIRED)" data/workerB.log | sed 's/^/    /'

echo "---- final alerts.log ----"
cat data/alerts.log | sed 's/^/    /'
N=$(wc -l < data/alerts.log 2>/dev/null || echo 0)
echo "[$(ts)] final alert count: $N (expected 2: one per distinct excursion, none lost, none doubled)"

# Stop worker B.
kill -9 "$WB" 2>/dev/null
echo "=== END ==="
