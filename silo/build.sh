#!/bin/bash
MODE=perf DEBUG=0 make -j4
MODE=perf DEBUG=0 make -j4 dbtest
