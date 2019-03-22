//
// Created by jiahua on 2019/3/13.
//

#include "../neatlib/basic_hash_table.h"
#include <chrono>
#include <random>
#include <vector>
#include <iostream>

using namespace std;
using namespace chrono;

constexpr size_t RANGE = 20000000;
constexpr size_t TOTAL_ELEMENTS = 10000000;

int main() {
    vector<size_t> keys(TOTAL_ELEMENTS, 0);
    neatlib::basic_hash_table<size_t,
                              size_t,
                              std::hash<size_t>,
                              std::equal_to<size_t>,
                              std::allocator<std::pair<const size_t, size_t>>,
                              8> bht;
    default_random_engine en(static_cast<unsigned int>(steady_clock::now().time_since_epoch().count()));
    uniform_int_distribution<size_t> dis(0, RANGE);
    std::size_t right = 0, right2 = 0, right3 = 0;
    for (auto &i : keys) i = dis(en);

    auto t1 = steady_clock::now();
    for (const auto &i : keys)
        bht.insert(i, 10);
    auto t2 = steady_clock::now();

    for (const auto &i : keys)
        if (bht.get(i).second == 10)
            ++right;

    auto t3 = steady_clock::now();

    for (const auto &i : keys)
        if (bht.update(i, 20))
            ++right2;
    auto t4 = steady_clock::now();

    for (const auto &i : keys)
        if (bht.remove(i))
            ++right3;

    auto t5 = steady_clock::now();

    cout << "INSERTION TIME: " << duration_cast<milliseconds>(t2 - t1).count() << endl;
    cout << "GETTING TIME:   " << duration_cast<milliseconds>(t3 - t2).count() << "  " << right << endl;
    cout << "UPDATING TIME:  " << duration_cast<milliseconds>(t4 - t3).count() << "  " << right2 << endl;
    cout << "REMOVING TIME:  " << duration_cast<milliseconds>(t5 - t4).count() << "  " << bht.size() << "  "
         << right3 << endl;

}

