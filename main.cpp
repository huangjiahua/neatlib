#include <atomic>
#include <memory>
#include <iostream>
#include <thread>
#include <exception>
#include "neatlib/concurrent_hash_table.h"
//#include "folly/concurrency/AtomicSharedPtr.h"


using namespace std;

int main() {

    neatlib::concurrent_hash_table<size_t,
                                   size_t,
                                   std::hash<size_t>,
                                   2,
                                   4> ht{};
    cout << boolalpha << ht.is_lock_free() << endl;
    for (size_t i = 0; i < 16; i++) {
        ht.insert(i, 10);
    }
    ht.insert(16, 10);
    ht.update(16, 55);
    auto p = ht.get(16);
    cout << ht.size() << endl;
    cout << p.second << endl;

    ht.remove(16);
    try {
        ht.get(16);
    } catch (std::exception &e) {
        cout << e.what() << endl;
    }
    return 0;
}