#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <utility>
#include <string>
#include <memory>

#include <stdlib.h>
#include <sched.h>
#include <unistd.h>
#include <sys/sysinfo.h>
#include <valarray>

#include "bench.h"

#include "../counter.h"
#include "../scopedperf.hh"
#include "../allocator.h"

#ifdef USE_JEMALLOC
//cannot include this header b/c conflicts with malloc.h
//#include <jemalloc/jemalloc.h>
extern "C" void malloc_stats_print(void (*write_cb)(void *, const char *), void *cbopaque, const char *opts);
extern "C" int mallctl(const char *name, void *oldp, size_t *oldlenp, void *newp, size_t newlen);
#endif
#ifdef USE_TCMALLOC
#include <google/heap-profiler.h>
#endif

#include "request.h"
#include "tbench_server.h"

#include "convergence.h"

using namespace std;
using namespace util;

size_t nthreads = 1;
volatile bool running = true;
int verbose = 0;
uint64_t txn_flags = 0;
double scale_factor = 1.0;
uint64_t ops_per_worker = 1000; // TODO remove this
int enable_parallel_loading = false;
int pin_cpus = 0;
int slow_exit = 0;
int retry_aborted_transaction = 0;
int no_reset_counters = 0;
int backoff_aborted_transaction = 0;

template<typename T>
static T getOpt(const char* name, T defVal) {
    const char* opt = getenv(name);

    std::cout << name << " = " << opt << std::endl;
    if (!opt) return defVal;
    std::stringstream ss(opt);
    if (ss.str().length() == 0) return defVal;
    T res;
    ss >> res;
    if (ss.fail()) {
        std::cerr << "WARNING: Option " << name << "(" << opt << ") could not"\
            << " be parsed, using default" << std::endl;
        return defVal;
    }   
    return res;
}

template<typename T>
static void
delete_pointers(const vector<T *> &pts) {
    for (size_t i = 0; i < pts.size(); i++)
        delete pts[i];
}

template<typename T>
static vector<T>
elemwise_sum(const vector<T> &a, const vector<T> &b) {
    INVARIANT(a.size() == b.size());
    vector<T> ret(a.size());
    for (size_t i = 0; i < a.size(); i++)
        ret[i] = a[i] + b[i];
    return ret;
}

template<typename K, typename V>
static void
map_agg(map<K, V> &agg, const map<K, V> &m) {
    for (auto it = m.begin();
         it != m.end(); ++it)
        agg[it->first] += it->second;
}

// returns <free_bytes, total_bytes>
static pair<uint64_t, uint64_t>
get_system_memory_info() {
    struct sysinfo inf;
    sysinfo(&inf);
    return make_pair(inf.mem_unit * inf.freeram, inf.mem_unit * inf.totalram);
}

static bool
clear_file(const char *name) {
    ofstream ofs(name);
    ofs.close();
    return true;
}

static void
write_cb(void *p, const char *s) UNUSED;

static void
write_cb(void *p, const char *s) {
    const char *f = "jemalloc.stats";
    static bool s_clear_file UNUSED = clear_file(f);
    ofstream ofs(f, ofstream::app);
    ofs << s;
    ofs.flush();
    ofs.close();
}

static event_avg_counter evt_avg_abort_spins("avg_abort_spins");

extern "C" void Client_changeDistribution(const int QPS);

void
bench_worker::run() {
    std::cout << "Starting Worker" << std::endl;

    if (set_core_id)
        coreid::set_core_id(worker_id);
    {
        scoped_rcu_region r;
    }
    on_run_setup();
    scoped_db_thread_ctx ctx(db, false);
    const workload_desc_vec workload = get_workload();
    txn_counts.resize(workload.size());
    barrier_a->count_down();
    barrier_b->wait_for();

    tBenchServerThreadStart();

    // Example of changing the distribution of all clients.
    const double baseLambda = getOpt<double>("TBENCH_QPS", 1000.0) * 1e-9;
    Client_changeDistribution(baseLambda);

    std::unique_ptr<IConvergenceModel> convergence_model(new VariationCoefficientModel(10.f, 5, 20));

    int count = 0;

    bool changed = false; // TODO remove
    std::vector<float> accumulator;

    while (running) {
        while (ntxn_commits < ops_per_worker) {
            Request *req;
            tBenchRecvReq(reinterpret_cast<void **>(&req));
            ReqType type = req->type;
            Response resp;
            retry:
            timer t;
            const unsigned long old_seed = r.get_seed();
            const auto ret = workload[req->type].fn(this);
            if (likely(ret.first)) {
                ++ntxn_commits;
                latency_numer_us += t.lap();
                backoff_shifts >>= 1;
                resp.success = true;
                tBenchSendResp(&resp, sizeof(resp));
            } else {
                ++ntxn_aborts;
                if (retry_aborted_transaction && running) {
                    if (backoff_aborted_transaction) {
                        if (backoff_shifts < 63)
                            backoff_shifts++;
                        uint64_t spins = 1UL << backoff_shifts;
                        spins *= 100; // XXX: tuned pretty arbitrarily
                        evt_avg_abort_spins.offer(spins);
                        while (spins) {
                            nop_pause();
                            spins--;
                        }
                    }
                    r.set_seed(old_seed);
                    goto retry;
                }
            }
            size_delta += ret.second; // should be zero on abort
            txn_counts[type]++;
        }

        float percentile = 99.0;                             // TODO: hardcoded
        float latency = tBenchServerDumpLatency(percentile);
        tail_latencies.push_back(latency);

        // @note Static just to keep it local here.
        static size_t step_index = 0;
        static const std::vector<int> steps = 
        {
            8000,
            8500,
            9000,
            9500,
            10000,
        };

        std::cout << "Latency : " << latency << std::endl;

        accumulator.push_back(latency);
        tail_latencies.clear();

        const bool bHasConverged = convergence_model->aggregate(latency);
        if (bHasConverged)
        {
            ++step_index;
            if (step_index >= steps.size())
            {
                std::cout << "Reached the end of the steps.\n";
                running = false;
            }
            else
            {
                const auto new_qps = steps[step_index];
                Client_changeDistribution(new_qps);
                convergence_model->reset();
                changed = true;
                std::cout << "Convergence reached. Increasing QPS to " << new_qps << "\n";
            }
        }

        // if (count > 30 && !changed) {
        //     int QPS = 13000;
        //     Client_changeDistribution(QPS);
        //     changed = true;
        // }

        // TODO reset state, ex ntx_commit
        ntxn_commits = 0;

        ++count;
    }
    std::ofstream outputFile("../output/output.txt");
    if (outputFile.is_open()) {
        for (size_t i = 0; i < accumulator.size(); ++i) {
            outputFile << accumulator[i];
            outputFile << "\n";
        }
        outputFile.close();
    } else {
        std::cout << "CANNOT OPEN FILE" << std::endl;
    }
}

void
bench_runner::run() {
    tBenchServerInit(nthreads);

    // load data
    const vector<bench_loader *> loaders = make_loaders();
    {
        spin_barrier b(loaders.size());
        const pair<uint64_t, uint64_t> mem_info_before = get_system_memory_info();
        {
            scoped_timer t("dataloading", verbose);
            for (auto loader: loaders) {
                loader->set_barrier(b);
                loader->start();
            }
            for (auto loader: loaders)
                loader->join();
        }
        const pair<uint64_t, uint64_t> mem_info_after = get_system_memory_info();
        const int64_t delta = int64_t(mem_info_before.first) - int64_t(mem_info_after.first); // free mem
        const double delta_mb = double(delta) / 1048576.0;
        if (verbose)
            cerr << "DB size: " << delta_mb << " MB" << endl;
    }

    db->do_txn_epoch_sync(); // also waits for worker threads to be persisted
    {
        const auto persisted_info = db->get_ntxn_persisted();
        if (get<0>(persisted_info) != get<1>(persisted_info))
            cerr << "ERROR: " << persisted_info << endl;
        //ALWAYS_ASSERT(get<0>(persisted_info) == get<1>(persisted_info));
        if (verbose)
            cerr << persisted_info << " txns persisted in loading phase" << endl;
    }
    db->reset_ntxn_persisted();

    if (!no_reset_counters) {
        event_counter::reset_all_counters(); // XXX: for now - we really should have a before/after loading
        PERF_EXPR(scopedperf::perfsum_base::resetall());
    }
    {
        const auto persisted_info = db->get_ntxn_persisted();
        if (get<0>(persisted_info) != 0 ||
            get<1>(persisted_info) != 0 ||
            get<2>(persisted_info) != 0.0) {
            cerr << persisted_info << endl;
            ALWAYS_ASSERT(false);
        }
    }

    map<string, size_t> table_sizes_before;
    if (verbose) {
        for (auto &open_table: open_tables) {
            scoped_rcu_region guard;
            const size_t s = open_table.second->size();
            cerr << "table " << open_table.first << " size " << s << endl;
            table_sizes_before[open_table.first] = s;
        }
        cerr << "starting benchmark..." << endl;
    }

    const pair<uint64_t, uint64_t> mem_info_before = get_system_memory_info();

    // ------------------------------------------------------------------

    // TODO here we need to determine the sample size for accurate tail latency measurement
    ops_per_worker = 7000;
    // in the beginning fixed size, later depending on observed variance

    const vector<bench_worker *> workers = make_workers();
    ALWAYS_ASSERT(!workers.empty());
    for (auto worker: workers)
        worker->start();

    barrier_a.wait_for(); // wait for all threads to start up
    timer t, t_nosync;
    barrier_b.count_down(); // bombs away!

    // TODO
    // ---------------
    // running = false;
    // logic block for convergence?
    // ---------------

    __sync_synchronize();
    for (size_t i = 0; i < nthreads; i++)
        workers[i]->join();
    const unsigned long elapsed_nosync = t_nosync.lap();
    db->do_txn_finish(); // waits for all worker txns to persist
    size_t n_commits = 0;
    size_t n_aborts = 0;
    uint64_t latency_numer_us = 0;
    for (size_t i = 0; i < nthreads; i++) {
        n_commits += workers[i]->get_ntxn_commits();
        n_aborts += workers[i]->get_ntxn_aborts();
        latency_numer_us += workers[i]->get_latency_numer_us();
    }
    const auto persisted_info = db->get_ntxn_persisted();

    const unsigned long elapsed = t.lap(); // lap() must come after do_txn_finish(),
    // because do_txn_finish() potentially
    // waits a bit

    tBenchServerFinish();
    // float curr_latency = parse_latencies(percentile); // TODO
    // bool converged = latency_convergence(prev_latency, curr_latency, confidence_interval);
    // if (converged && curr_latency < latency_requirement) {
    //      increase_load();
    //      continue;
    // } else if (converged && curr_latency > latency_requirement) {
    //      return prev_latency;
    // }
    // TODO we then decided if we increase workload or not
    // TODO here is the logic to stop the workload when we have converged
    // ------------------------------------------------------------------


    // various sanity checks
    ALWAYS_ASSERT(get<0>(persisted_info) == get<1>(persisted_info));
    // not == b/c persisted_info does not count read-only txns
    ALWAYS_ASSERT(n_commits >= get<1>(persisted_info));

    const double elapsed_nosync_sec = double(elapsed_nosync) / 1000000.0;
    const double agg_nosync_throughput = double(n_commits) / elapsed_nosync_sec;
    const double avg_nosync_per_core_throughput = agg_nosync_throughput / double(workers.size());

    const double elapsed_sec = double(elapsed) / 1000000.0;
    const double agg_throughput = double(n_commits) / elapsed_sec;
    const double avg_per_core_throughput = agg_throughput / double(workers.size());

    const double agg_abort_rate = double(n_aborts) / elapsed_sec;
    const double avg_per_core_abort_rate = agg_abort_rate / double(workers.size());

    // we can use n_commits here, because we explicitly wait for all txns
    // run to be durable
    const double agg_persist_throughput = double(n_commits) / elapsed_sec;
    const double avg_per_core_persist_throughput =
            agg_persist_throughput / double(workers.size());

    // XXX(stephentu): latency currently doesn't account for read-only txns
    const double avg_latency_us =
            double(latency_numer_us) / double(n_commits);
    const double avg_latency_ms = avg_latency_us / 1000.0;
    const double avg_persist_latency_ms =
            get<2>(persisted_info) / 1000.0;

    if (verbose) {
        const pair<uint64_t, uint64_t> mem_info_after = get_system_memory_info();
        const int64_t delta = int64_t(mem_info_before.first) - int64_t(mem_info_after.first); // free mem
        const double delta_mb = double(delta) / 1048576.0;
        map<string, size_t> agg_txn_counts = workers[0]->get_txn_counts();
        ssize_t size_delta = workers[0]->get_size_delta();
        for (size_t i = 1; i < workers.size(); i++) {
            map_agg(agg_txn_counts, workers[i]->get_txn_counts());
            size_delta += workers[i]->get_size_delta();
        }
        const double size_delta_mb = double(size_delta) / 1048576.0;
        map<string, counter_data> ctrs = event_counter::get_all_counters();

        cerr << "--- table statistics ---" << endl;
        for (auto &open_table: open_tables) {
            scoped_rcu_region guard;
            const size_t s = open_table.second->size();
            const ssize_t delta = ssize_t(s) - ssize_t(table_sizes_before[open_table.first]);
            cerr << "table " << open_table.first << " size " << open_table.second->size();
            if (delta < 0)
                cerr << " (" << delta << " records)" << endl;
            else
                cerr << " (+" << delta << " records)" << endl;
        }

#ifdef ENABLE_BENCH_TXN_COUNTERS
        cerr << "--- txn counter statistics ---" << endl;
        {
          // take from thread 0 for now
          abstract_db::txn_counter_map agg = workers[0]->get_local_txn_counters();
          for (auto &p : agg) {
            cerr << p.first << ":" << endl;
            for (auto &q : p.second)
              cerr << "  " << q.first << " : " << q.second << endl;
          }
        }
#endif
        cerr << "--- benchmark statistics ---" << endl;
        cerr << "runtime: " << elapsed_sec << " sec" << endl;
        cerr << "memory delta: " << delta_mb << " MB" << endl;
        cerr << "memory delta rate: " << (delta_mb / elapsed_sec) << " MB/sec" << endl;
        cerr << "logical memory delta: " << size_delta_mb << " MB" << endl;
        cerr << "logical memory delta rate: " << (size_delta_mb / elapsed_sec) << " MB/sec" << endl;
        cerr << "agg_nosync_throughput: " << agg_nosync_throughput << " ops/sec" << endl;
        cerr << "avg_nosync_per_core_throughput: " << avg_nosync_per_core_throughput << " ops/sec/core" << endl;
        cerr << "agg_throughput: " << agg_throughput << " ops/sec" << endl;
        cerr << "avg_per_core_throughput: " << avg_per_core_throughput << " ops/sec/core" << endl;
        cerr << "agg_persist_throughput: " << agg_persist_throughput << " ops/sec" << endl;
        cerr << "avg_per_core_persist_throughput: " << avg_per_core_persist_throughput << " ops/sec/core" << endl;
        cerr << "avg_latency: " << avg_latency_ms << " ms" << endl;
        cerr << "avg_persist_latency: " << avg_persist_latency_ms << " ms" << endl;
        cerr << "agg_abort_rate: " << agg_abort_rate << " aborts/sec" << endl;
        cerr << "avg_per_core_abort_rate: " << avg_per_core_abort_rate << " aborts/sec/core" << endl;
        cerr << "txn breakdown: " << format_list(agg_txn_counts.begin(), agg_txn_counts.end()) << endl;
        cerr << "--- system counters (for benchmark) ---" << endl;
        for (auto &ctr: ctrs)
            cerr << ctr.first << ": " << ctr.second << endl;
        cerr << "--- perf counters (if enabled, for benchmark) ---" << endl;
        PERF_EXPR(scopedperf::perfsum_base::printall());
        cerr << "--- allocator stats ---" << endl;
        ::allocator::DumpStats();
        cerr << "---------------------------------------" << endl;

    }

    // output for plotting script
    cout << agg_throughput << " "
         << agg_persist_throughput << " "
         << avg_latency_ms << " "
         << avg_persist_latency_ms << " "
         << agg_abort_rate << endl;
    cout.flush();

    if (!slow_exit)
        return;

    map<string, uint64_t> agg_stats;
    for (auto &open_table: open_tables) {
        map_agg(agg_stats, open_table.second->clear());
        delete open_table.second;
    }
    if (verbose) {
        for (auto &p: agg_stats)
            cerr << p.first << " : " << p.second << endl;

    }
    open_tables.clear();

    delete_pointers(loaders);
    delete_pointers(workers); // moved up in loop
}

template<typename K, typename V>
struct map_maxer {
    typedef map<K, V> map_type;

    void
    operator()(map_type &agg, const map_type &m) const {
        for (typename map_type::const_iterator it = m.begin();
             it != m.end(); ++it)
            agg[it->first] = std::max(agg[it->first], it->second);
    }
};


#ifdef ENABLE_BENCH_TXN_COUNTERS
void
bench_worker::measure_txn_counters(void *txn, const char *txn_name)
{
  auto ret = db->get_txn_counters(txn);
  map_maxer<string, uint64_t>()(local_txn_counters[txn_name], ret);
}
#endif

map<string, size_t>
bench_worker::get_txn_counts() const {
    map<string, size_t> m;
    const workload_desc_vec workload = get_workload();
    for (size_t i = 0; i < txn_counts.size(); i++)
        m[workload[i].name] = txn_counts[i];
    return m;
}
