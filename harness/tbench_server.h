/** $lic$
 * Copyright (C) 2016-2017 by Massachusetts Institute of Technology
 *
 * This file is part of TailBench.
 *
 * If you use this software in your research, we request that you reference the
 * TaiBench paper ("TailBench: A Benchmark Suite and Evaluation Methodology for
 * Latency-Critical Applications", Kasture and Sanchez, IISWC-2016) as the
 * source in any publications that use this software, and that you send us a
 * citation of your work.
 *
 * TailBench is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.
 */

#ifndef __TBENCH_SERVER_H
#define __TBENCH_SERVER_H

#include <stdlib.h>

#ifdef __cplusplus
extern "C" {
#endif

void tBenchServerInit(int nthreads);

void tBenchServerThreadStart();

void tBenchServerFinish();

size_t tBenchRecvReq(void **data);

void tBenchSendResp(const void *data, size_t size);

float tBenchServerDumpLatency(float percentile);

double tBenchServerDumpVariance();

double tBenchServerDumpMean();

int tBenchServerGetStatus();

size_t tBenchServerDumpSampleSize();

size_t tBenchServerDumpAggregateLatency(float percentile, int window);

double tBenchServerDumpAggregateVariance(int window, double mean);

double tBenchServerDumpAggregateMean(int window);

#ifdef __cplusplus
}
#endif

#endif
