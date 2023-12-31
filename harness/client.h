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

#ifndef __CLIENT_H
#define __CLIENT_H

#include "msgs.h"
#include "dist.h"

#include <pthread.h>
#include <stdint.h>
#include <map>

#include <string>
#include <unordered_map>
#include <vector>

enum ClientStatus { INIT, WARMUP, ROI, FINISHED };

class Client {
    protected:
        ClientStatus status;

        int nthreads;
        pthread_mutex_t lock;
        pthread_barrier_t barrier;

        uint64_t minSleepNs;
        uint64_t seed;
        double lambda;
        ExpDist* dist;

        uint64_t startedReqs;
        std::unordered_map<uint64_t, Request*> inFlightReqs;

        std::vector<uint64_t> svcTimes;
        std::vector<uint64_t> queueTimes;
        std::vector<uint64_t> sjrnTimes;
        std::vector<std::vector<uint64_t>> aggregateSjrn;

        void _startRoi();

    public:
        //Client(int nthreads);

        Client(int nthreads, uint64_t precision);

        Request* startReq();
        void finiReq(Response* resp);

        void startRoi();
        void dumpStats();
        float dumpLatency(float percentile);


// ######################################################################
// ###              LOADBOIZ begin change
    public:
        std::map<uint64_t, uint64_t> bins{};
        uint64_t precision;
        uint64_t reqs;
        uint64_t warmup_count;

        /* Changes the distribution of all clients. */
        static void changeDistribution(const double QPS);
        enum ClientStatus getStatus();
        size_t QPS();
        uint64_t Reqs();
        double getAggregateVariance(double mean);
        double getAggregateMean();
        double getAggregateLatency(float percentile);
        float getSampleLatency(float percentile);
        float getSampleVariance();

        protected:
            void overrideIfDirty();
            static double lambda_override;
            // ###              LOADBOIZ end change
            // ######################################################################
        };

/*class NetworkedClient : public Client {
    private:
        pthread_mutex_t sendLock;
        pthread_mutex_t recvLock;

        int serverFd;
        std::string error;

    public:
        NetworkedClient(int nthreads, std::string serverip, int serverport);
        bool send(Request* req);
        bool recv(Response* resp);
        const std::string& errmsg() const { return error; }
};*/

#endif
