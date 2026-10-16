#!/usr/bin/env bash
# Launch the ColdLedger UI on loopback, detached, and return immediately.
cd ~/build/coldledger
rm -f ./index.html
. .venv/bin/activate
setsid uvicorn app:app --host 127.0.0.1 --port 8010 > data/uvicorn.log 2>&1 < /dev/null &
disown
echo "serve launched pid $!"
