#include <iostream>
#include <array>
#include <memory>
#include "neatlib/basic_hash_table.h"

using namespace std;
int main() {
    std::cout << "Hello, World!" << std::endl;
    cout << sizeof(array<int, 32>) << endl;
    cout << sizeof(unique_ptr<int>) << endl;

    std::unique_ptr<std::array<int, 32>> ptr(make_unique<std::array<int, 32>>());

    neatlib::basic_hash_table<int, int> ht(300);
    ht.insert(1, 1);

    return 0;
}