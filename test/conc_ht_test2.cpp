//
// Created by jiahua on 2019/3/19.
//
#include <atomic>
#include <memory>
#include <iostream>
#include <thread>
#include <vector>
#include <chrono>
#include <random>
#include "../neatlib/concurrent_hash_table.h"
#include <functional>
#include <folly/concurrency/AtomicSharedPtr.h>


using namespace std;
using namespace chrono;

constexpr size_t RANGE = 20000000;
constexpr size_t TOTAL_ELEMENTS = 10000000;
constexpr size_t threadNum = 12;

template <typename HT>
void insert_task(HT &ht, vector<size_t> &keys, size_t threadIdx) {
    for ( ; threadIdx < keys.size(); threadIdx += threadNum)
        ht.insert(keys[threadIdx], 10);
}

template <typename HT>
void get_task(HT &ht, vector<size_t> &keys, size_t threadIdx) {
    for ( ; threadIdx < keys.size(); threadIdx += threadNum)
        ht.get(keys[threadIdx]);
}

template <typename HT>
void update_task(HT &ht, vector<size_t> &keys, size_t threadIdx) {
    for ( ; threadIdx < keys.size(); threadIdx += threadNum)
        ht.update(keys[threadIdx], 55);
}

template <typename HT>
void remove_task(HT &ht, vector<size_t> &keys, size_t threadIdx) {
    for (; threadIdx < keys.size(); threadIdx += threadNum) {
        ht.remove(keys[threadIdx]);
    }
}

template <typename T> using lockfree_atomic_shared_ptr = folly::atomic_shared_ptr<T>;
template <typename T> using lockfree_shared_ptr = folly::detail::shared_ptr_internals::CountedPtr<T>;

int main() {
    vector<size_t> keys(TOTAL_ELEMENTS, 0);
    vector<thread> threads(threadNum);
    neatlib::concurrent_hash_table<size_t,
                                   size_t,
                                   std::hash<size_t>,
                                   4,
                                   8,
                                   lockfree_atomic_shared_ptr,
                                   lockfree_shared_ptr> ht{};
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

    ht.insert(16, 10);
    cout << "TOTAL SIZE:      " << ht.size() << endl;
    cout << "INSERTION TIME:  " << duration_cast<milliseconds>(t2 - t1).count() << endl;
    cout << "GETTING TIME:    " << duration_cast<milliseconds>(t3 - t2).count() << endl;
    cout << "UPDATING TIME:   " << duration_cast<milliseconds>(t4 - t3).count() << endl;
    cout << "REMOVING TIME:   " << duration_cast<milliseconds>(t5 - t4).count() << endl;
    return 0;
}
