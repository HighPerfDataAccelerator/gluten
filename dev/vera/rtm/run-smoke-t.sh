#!/bin/bash
# usage: run-smoke-t.sh <label> <conf.json> <suite> <queries> [timeout_s]  -- run-smoke with hang cleanup
cd /data/nvme1/rtm/exp
timeout ${5:-300} ./run-smoke.sh "$1" "$2" "$3" "$4" > "$1.log" 2>&1; rc=$?
if pgrep -f "[d]eploy.worker.Worker|[S]parkSubmit|[C]oarseGrainedExecutorBackend" >/dev/null; then
  pkill -f "[r]un-suite-rtm.py"; pkill -f "[S]parkSubmit"; sleep 3; pkill -f "[C]oarseGrainedExecutorBackend"
  pkill -f "[d]eploy.worker.Worker"; pkill -f "[d]eploy.master.Master"; sleep 8; echo "$1 cleaned up after rc=$rc"
fi
echo "$1 rc=$rc"
python3 -c "import json,sys;[print(\"  \",q[\"query\"],q[\"status\"],round(q.get(\"seconds\") or 0,2)) for q in json.load(open(sys.argv[1]))]" results/$1-$3-candidate/queries.json 2>/dev/null
