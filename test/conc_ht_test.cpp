//
// Created by jiahua on 2019/3/19.
//
#include <atomic>
#include <string>
#include <memory>
#include <iostream>
#include <thread>
#include <vector>
#include <chrono>
#include <random>
#include "../neatlib/concurrent_hash_table.h"
#include "neatlib/lock_free_hash_table.h"
#include <functional>


using namespace std;
using namespace chrono;

size_t RANGE = 20000000;
size_t TOTAL_ELEMENTS = 10000000;
size_t threadNum = 12;

template <typename HT>
void insert_task(HT &ht, vector<size_t> &keys, size_t threadIdx) {
    for ( ; threadIdx < keys.size(); threadIdx += threadNum)
        ht.Insert(keys[threadIdx], 10);
}

template <typename HT>
void get_task(HT &ht, vector<size_t> &keys, size_t threadIdx) {
    for ( ; threadIdx < keys.size(); threadIdx += threadNum)
        ht.Get(keys[threadIdx]);
}

template <typename HT>
void update_task(HT &ht, vector<size_t> &keys, size_t threadIdx) {
    for ( ; threadIdx < keys.size(); threadIdx += threadNum)
        ht.Update(keys[threadIdx], 55);
}

template <typename HT>
void remove_task(HT &ht, vector<size_t> &keys, size_t threadIdx) {
    for (; threadIdx < keys.size(); threadIdx += threadNum) {
        ht.Remove(keys[threadIdx]);
    }
}

int main(int argc, const char *argv[]) {
    if (argc >= 2) threadNum = stoi(string(argv[1]));
    if (argc >= 3) TOTAL_ELEMENTS = stoi(string(argv[2]));
    if (argc >= 4) RANGE = stoi(string(argv[3]));
    vector<size_t> keys(TOTAL_ELEMENTS, 0);
    vector<thread> threads(threadNum);
    neatlib::ConcurrentHashTable<size_t,
                                   size_t,
                                   std::hash<size_t>,
                                   4,
                                   8> ht{};
    default_random_engine en(static_cast<unsigned int>(steady_clock::now().time_since_epoch().count()));
    uniform_int_distribution<size_t> dis(0, RANGE);
    std::size_t right = 0, right2 = 0, right3 = 0;
    for (auto &i : keys) i = dis(en);

    auto t1 = steady_clock::now();
    for (size_t i = 0; i < threadNum; i++) {
        threads[i] = thread(insert_task<decltype(ht)>, std::ref(ht), std::ref(keys), i);
    }
    for (auto &t : threads) {
        t.join();
    }
    auto t2 = steady_clock::now();
    for (size_t i = 0; i < threadNum; i++) {
        threads[i] = thread(get_task<decltype(ht)>, std::ref(ht), std::ref(keys), i);
    }
    for (auto &t : threads) {
        t.join();
    }
    auto t3 = steady_clock::now();
    for (size_t i = 0; i < threadNum; i++) {
        threads[i] = thread(update_task<decltype(ht)>, std::ref(ht), std::ref(keys), i);
    }
    for (auto &t : threads) {
        t.join();
    }
    auto t4 = steady_clock::now();
    for (size_t i = 0; i < threadNum; i++) {
        threads[i] = thread(remove_task<decltype(ht)>, std::ref(ht), std::ref(keys), i);
    }
    for (auto &t : threads) {
        t.join();
    }
    auto t5 = steady_clock::now();

    ht.Insert(16, 10);
    cout << "ThreadNum:       " << threadNum << endl;
    cout << "INSERTION TIME:  " << duration_cast<milliseconds>(t2 - t1).count() << endl;
    cout << "GETTING TIME:    " << duration_cast<milliseconds>(t3 - t2).count() << endl;
    cout << "UPDATING TIME:   " << duration_cast<milliseconds>(t4 - t3).count() << endl;
    cout << "REMOVING TIME:   " << duration_cast<milliseconds>(t5 - t4).count() << endl;
    return 0;
}
