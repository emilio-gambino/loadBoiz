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

#include "client.h"
#include "helpers.h"
#include "tbench_client.h"

#include <assert.h>
#include <errno.h>
#include <string.h>
#include <sys/select.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <unistd.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

/*******************************************************************************
 * Client
 *******************************************************************************/

// ######################################################################
// ###              LOADBOIZ begin change

// Workaround to not include client.h as they define the same struct in bench.h and msg.h with
// different members.
void Client_changeDistribution(const double lambda) {
    std::cout << "Changing distribution to " << lambda * 1e9 << "\n";
    Client::changeDistribution(lambda);
}

double Client::lambda_override; // Set in constructor

void Client::changeDistribution(const double QPS) {
    lambda_override = QPS * 1e-9;
}

void Client::overrideIfDirty() {
    const bool bDirty = lambda != lambda_override;
    if (bDirty) {
        const uint64_t curNs = getCurNs();

        lambda = lambda_override;

        if (dist) {
            delete dist;
            dist = nullptr;
        }

        dist = new ExpDist(lambda, seed, curNs);
        std::cout << "-------------- Changing QPS to: " << lambda * 1e9 << " --------------" << std::endl;
    }
}

double Client::getMean() {
    double m = 0;
    for (uint64_t value: sjrnTimes) {
        m += static_cast<double>(value);
    }
    m /= static_cast<double>(sjrnTimes.size());

    return m;
}

double Client::getVariance() {
    double mean = getMean();

    double sumSqr = 0;
    for (uint64_t value: sjrnTimes) {
        double diff = value - mean;
        sumSqr += diff * diff;
    }

    double var = sumSqr / (sjrnTimes.size() - 1);
    return var;
}

// input float percentile : a number between 1 and 100
float Client::dumpLatency(float percentile) {
    sort(sjrnTimes.begin(), sjrnTimes.end());
    uint64_t lat = sjrnTimes[(percentile / 100) * sjrnTimes.size()];
    sjrnTimes.clear();
    // TODO also parse, queue times and service times
    queueTimes.clear();
    svcTimes.clear();
    return (float) lat;
}

ClientStatus Client::getStatus() {
    return status;
}

// ###              LOADBOIZ end change
// ######################################################################

Client::Client(int _nthreads) {
    status = INIT;

    nthreads = _nthreads;
    pthread_mutex_init(&lock, nullptr);
    pthread_barrier_init(&barrier, nullptr, nthreads);

    minSleepNs = getOpt("TBENCH_MINSLEEPNS", 0);
    seed = getOpt("TBENCH_RANDSEED", 0);
    lambda = getOpt<double>("TBENCH_QPS", 1000.0) * 1e-9;
    lambda_override = lambda;

    dist = nullptr; // Will get initialized in startReq()

    startedReqs = 0;


    tBenchClientInit();
}

Request *Client::startReq() {
    if (status == INIT) {
        pthread_barrier_wait(&barrier); // Wait for all threads to start up

        pthread_mutex_lock(&lock);

        if (!dist) {
            uint64_t curNs = getCurNs();
            dist = new ExpDist(lambda, seed, curNs);

            std::cout << "Starting Warmup .." << std::endl;
            status = WARMUP;

            pthread_barrier_destroy(&barrier);
            pthread_barrier_init(&barrier, nullptr, nthreads);
        }

        pthread_mutex_unlock(&lock);

        pthread_barrier_wait(&barrier);
    }

    pthread_mutex_lock(&lock);

    // ######################################################################
    // ###              LOADBOIZ begin change
    overrideIfDirty();
    // ###              LOADBOIZ end change
    // ######################################################################

    Request *req = new Request();
    size_t len = tBenchClientGenReq(&req->data);
    req->len = len;

    req->id = startedReqs++;
    req->genNs = dist->nextArrivalNs();
    inFlightReqs[req->id] = req;

    pthread_mutex_unlock(&lock);

    uint64_t curNs = getCurNs();

    if (curNs < req->genNs) {
        sleepUntil(std::max(req->genNs, curNs + minSleepNs));
    }

    return req;
}

void Client::finiReq(Response *resp) {
    pthread_mutex_lock(&lock);

    auto it = inFlightReqs.find(resp->id);
    assert(it != inFlightReqs.end());
    Request *req = it->second;

    if (status == ROI || status == WARMUP) {
        uint64_t curNs = getCurNs();

        assert(curNs > req->genNs);

        uint64_t sjrn = curNs - req->genNs;
        assert(sjrn >= resp->svcNs);
        uint64_t qtime = sjrn - resp->svcNs;

        queueTimes.push_back(qtime);
        svcTimes.push_back(resp->svcNs);
        sjrnTimes.push_back(sjrn);
    }

    delete req;
    inFlightReqs.erase(it);
    pthread_mutex_unlock(&lock);
}

void Client::_startRoi() {
    assert(status == WARMUP);
    status = ROI;

    queueTimes.clear();
    svcTimes.clear();
    sjrnTimes.clear();
}

void Client::startRoi() {
    pthread_mutex_lock(&lock);
    _startRoi();
    pthread_mutex_unlock(&lock);
}

void Client::dumpStats() {
    std::ofstream out("lats.bin", std::ios::out | std::ios::binary);
    int reqs = sjrnTimes.size();

    for (int r = 0; r < reqs; ++r) {
        out.write(reinterpret_cast<const char *>(&queueTimes[r]),
                  sizeof(queueTimes[r]));
        out.write(reinterpret_cast<const char *>(&svcTimes[r]),
                  sizeof(svcTimes[r]));
        out.write(reinterpret_cast<const char *>(&sjrnTimes[r]),
                  sizeof(sjrnTimes[r]));
    }
    out.close();
}


/*******************************************************************************
 * Networked Client
 *******************************************************************************/
NetworkedClient::NetworkedClient(int nthreads, std::string serverip,
                                 int serverport) : Client(nthreads) {
    pthread_mutex_init(&sendLock, nullptr);
    pthread_mutex_init(&recvLock, nullptr);

    // Get address info
    int status;
    struct addrinfo hints;
    struct addrinfo *servInfo;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    std::stringstream portstr;
    portstr << serverport;

    const char *serverStr = serverip.size() ? serverip.c_str() : nullptr;

    if ((status = getaddrinfo(serverStr, portstr.str().c_str(), &hints,
                              &servInfo)) != 0) {
        std::cerr << "getaddrinfo() failed: " << gai_strerror(status) \
 << std::endl;
        exit(-1);
    }

    serverFd = socket(servInfo->ai_family, servInfo->ai_socktype, \
            servInfo->ai_protocol);
    if (serverFd == -1) {
        std::cerr << "socket() failed: " << strerror(errno) << std::endl;
        exit(-1);
    }

    if (connect(serverFd, servInfo->ai_addr, servInfo->ai_addrlen) == -1) {
        std::cerr << "connect() failed: " << strerror(errno) << std::endl;
        exit(-1);
    }

    int nodelay = 1;
    if (setsockopt(serverFd, IPPROTO_TCP, TCP_NODELAY,
                   reinterpret_cast<char *>(&nodelay), sizeof(nodelay)) == -1) {
        std::cerr << "setsockopt(TCP_NODELAY) failed: " << strerror(errno) \
 << std::endl;
        exit(-1);
    }
}

bool NetworkedClient::send(Request *req) {
    pthread_mutex_lock(&sendLock);

    int len = sizeof(Request) - MAX_REQ_BYTES + req->len;
    int sent = sendfull(serverFd, reinterpret_cast<const char *>(req), len, 0);
    if (sent != len) {
        error = strerror(errno);
    }

    pthread_mutex_unlock(&sendLock);

    return (sent == len);
}

bool NetworkedClient::recv(Response *resp) {
    pthread_mutex_lock(&recvLock);

    int len = sizeof(Response) - MAX_RESP_BYTES; // Read request header first
    int recvd = recvfull(serverFd, reinterpret_cast<char *>(resp), len, 0);
    if (recvd != len) {
        error = strerror(errno);
        return false;
    }

    if (resp->type == RESPONSE) {
        recvd = recvfull(serverFd, reinterpret_cast<char *>(&resp->data), \
                resp->len, 0);

        if (static_cast<size_t>(recvd) != resp->len) {
            error = strerror(errno);
            return false;
        }
    }

    pthread_mutex_unlock(&recvLock);

    return true;
}

