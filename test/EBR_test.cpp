//
// Created by jiahua on 2019/5/12.
//

#include "../epoch/memory_epoch.h"
#include <vector>
#include <string>
#include <iostream>
#include <random>
#include <thread>
#include <atomic>
#include <cstdlib>
#include <cstdint>

using namespace std;
using atomic_p = atomic<uint64_t*>;

vector<atomic_p> data;
vector<size_t> ops;
vector<int> coin;
size_t task_num = 1000000000, data_num = 1000, thread_num = 4;
neatlib::epoch::MemoryEpoch<uint64_t>* epoch = nullptr;

void init() {
    data = vector<atomic_p>(data_num);
    for (auto &a : data) a.store(new uint64_t(10));
    ops.reserve(data_num * 1000);
    coin.reserve(data_num * 1000);
    mt19937 en(chrono::steady_clock::now().time_since_epoch().count());
    uniform_int_distribution<uint64_t> dis(0, data.size());
    uniform_int_distribution<uint64_t> c(0, 1);
    for (size_t i = 0; i < data_num * 1000; i++) {
        ops.push_back(dis(en));
        coin.push_back(c(en));
    }
}

inline uint64_t read(size_t idx) {
    auto p = data[idx].load();
    auto val = *p;
    val += idx;
    return val;
}

inline uint64_t* update(size_t idx) {
    auto np = new uint64_t(42);
    auto p = data[idx].load();
    if (!data[idx].compare_exchange_strong(p, np)) exit(1);
    return p;
}

void unsafe_thread_task(size_t idx) {
    for (size_t i = idx; i < task_num; i += thread_num) {
        int write = coin[idx % coin.size()];
        size_t loc = ops[idx % ops.size()];
        if (write) {
            update(loc);
        } else {
            read(loc);
        }
    }
}

void epoch_thread_task(size_t idx) {
    for (size_t i = idx; i < task_num; i += thread_num) {
        int write = coin[idx % coin.size()];
        size_t loc = ops[idx % ops.size()];
        if (write) {
            uint64_t *p = update(idx);
            epoch->BumpEpoch(p);
        } else {
            read(loc);
        }
        epoch->UpdateEpoch();
    }
}

int main(int argc, const char *argv[]) {
    init();
    vector<thread> threads(thread_num);
    epoch = new neatlib::epoch::MemoryEpoch<uint64_t>(thread_num);

    auto t1 = chrono::steady_clock::now();

    for (int i = 0; i < threads.size(); i++) {
        threads[i] = thread(epoch_thread_task, i);
    }

    for (auto &t : threads) t.join();

    auto t2 = chrono::steady_clock::now();

    cout << chrono::duration_cast<chrono::microseconds>(t2 - t1).count() / 1000000 << endl;
    return 0;
}

