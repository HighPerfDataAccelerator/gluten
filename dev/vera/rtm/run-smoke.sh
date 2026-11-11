#!/bin/bash
# usage: run-smoke.sh <label> <conf.json> <suite> <queries>
set -e
cd /data/nvme1/rtm/exp
busy=$(ps -eo args | grep -c "[C]oarseGrainedExecutorBackend\|[d]eploy.master.Master\|[d]eploy.worker.Worker" || true)
[ "$busy" = 0 ] || { echo "cluster busy ($busy)"; exit 3; }
XDG_RUNTIME_DIR=/run/user/1000 python3 run-suite-rtm.py $3 candidate $1 --conf-json $2 --queries $4
