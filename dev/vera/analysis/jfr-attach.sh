#!/bin/bash
# wait for the benchmark driver JVM and start a JFR recording dumped at exit
for i in $(seq 1 600); do
  P=$(pgrep -f "[S]parkSubmit.*benchmark_scale" | head -1); [ -z "$P" ] && P=$(pgrep -f "[o]rg.apache.spark.deploy.SparkSubmit" | head -1)
  if [ -n "$P" ]; then sleep 5; J=$(readlink -f /proc/$P/exe | sed "s#/bin/java##"); $J/bin/jcmd $P JFR.start name=drv settings=profile dumponexit=true filename=/data/nvme1/rtm/jfr/$1.jfr; echo "attached $P"; exit 0; fi
  sleep 1
done
