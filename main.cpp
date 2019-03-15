#include <atomic>
#include <memory>
#include <iostream>

using namespace std;

int main() {

    cout << atomic<size_t>().is_lock_free() << endl;

    return 0;
}