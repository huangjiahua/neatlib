#include <atomic>
#include <memory>
#include <iostream>
#include <thread>
#include <boost/smart_ptr/make_shared.hpp>
#include <boost/smart_ptr/shared_ptr.hpp>
#include <boost/smart_ptr/atomic_shared_ptr.hpp>

using namespace std;

int main() {

    boost::atomic_shared_ptr<int> ptr(boost::make_shared<int>(3));

    boost::shared_ptr<int> tmp_p = ptr.load();

    cout << *tmp_p << endl;

    return 0;
}