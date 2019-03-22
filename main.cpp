#include <atomic>
#include <memory>
#include <iostream>
#include <thread>
#include "neatlib/concurrent_hash_table.h"


using namespace std;

int main() {

    neatlib::concurrent_hash_table<size_t,
                                   size_t,
                                   std::hash<size_t>,
                                   std::equal_to<std::size_t>,
                                   std::allocator<std::pair<const size_t, const size_t>>,
                                   2,
                                   4> ht{};
    for (size_t i = 0; i < 16; i++) {
        ht.insert(i, 10);
    }
    ht.insert(16, 10);
    auto p = ht.get(16);
    cout << ht.size() << endl;
    cout << p.second << endl;
    return 0;
}