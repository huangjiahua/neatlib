//
// Created by jiahua on 2019/5/21.
//
#include "../neatlib/lock_free_hash_table.h"
#include <iostream>
using namespace std;

int main() {
    neatlib::LockFreeHashTable<int, int> ht;
    for (int i = 0; i < 500; i++) ht.Insert(i, i);
    auto res = ht.Get(322);
    cout << res.second << endl;
    ht.Update(322, 777);
    auto res2 = ht.Get(322);
    cout << res2.second << endl;
    ht.Remove(322);
    try {
        ht.Get(322);
    } catch(const std::out_of_range &e) {
        cout << e.what() << endl;
    }
}

