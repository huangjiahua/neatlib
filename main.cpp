#include <atomic>
#include <memory>
#include <iostream>
#include <thread>
#include "neatlib/concurrent_hast_table.h"


using namespace std;

int main() {

    neatlib::concurrent_hast_table<size_t, size_t> ht;
    for (size_t i = 0; i < 16; i++) {
        ht.insert(i, 10);
    }

    ht.insert(16, 10);
    auto p = ht.get(16);
    cout << ht.size() << endl;
    cout << p.second << endl;
    return 0;
}