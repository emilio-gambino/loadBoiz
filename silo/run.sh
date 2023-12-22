#!/bin/bash
# ops-per-worker is set to a very large value, so that TBENCH_MAXREQS controls how
# many ops are performed
NUM_WAREHOUSES=1
NUM_THREADS=1


QPS=4000
MAXREQS=20000000
WARMUPREQS=20001 # Must be > 0

TBENCH_QPS=${QPS} TBENCH_MAXREQS=${MAXREQS} TBENCH_WARMUPREQS=${WARMUPREQS} TBENCH_MINSLEEPNS=100 TBENCH_RANDSEED=0 ./out-perf.masstree/benchmarks/dbtest_integrated --verbose \
    --bench tpcc --num-threads ${NUM_THREADS} --scale-factor ${NUM_WAREHOUSES} --retry-aborted-transactions 
    
#TBENCH_QPS=${QPS} TBENCH_MAXREQS=${MAXREQS} TBENCH_WARMUPREQS=${WARMUPREQS} TBENCH_MINSLEEPNS=100 TBENCH_RANDSEED=0 gdb --args ./out-perf.debug.masstree/benchmarks/dbtest_integrated --verbose \
#    --bench bid --num-threads ${NUM_THREADS} --scale-factor ${NUM_WAREHOUSES} --retry-aborted-transactions 


#TBENCH_QPS=${QPS} TBENCH_MAXREQS=${MAXREQS} TBENCH_WARMUPREQS=${WARMUPREQS} \
#    TBENCH_MINSLEEPNS=10000 \
#    gdb --args ./out-perf.debug.masstree/benchmarks/dbtest_integrated --verbose \
#    --bench tpcc --num-threads ${NUM_THREADS} --scale-factor ${NUM_WAREHOUSES} \
#    --ops-per-worker 10000000
#TBENCH_QPS=${QPS} TBENCH_MAXREQS=${MAXREQS} TBENCH_WARMUPREQS=${WARMUPREQS} \
#    TBENCH_MINSLEEPNS=10000 chrt -r 99 \
#    ./out-perf.masstree/benchmarks/dbtest_integrated --verbose \
#    --bench tpcc --num-threads ${NUM_THREADS} --scale-factor ${NUM_WAREHOUSES} \
#    --retry-aborted-transactions --ops-per-worker 10000000
