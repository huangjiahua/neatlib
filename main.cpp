#include <atomic>
#include <memory>
#include <iostream>
#include <thread>
#include "neatlib/concurrent_hast_table.h"


using namespace std;

int main() {

    neatlib::concurrent_hast_table<size_t, size_t> ht;
    ht.insert(1, 1);
    cout << ht.size() << endl;
    return 0;
}