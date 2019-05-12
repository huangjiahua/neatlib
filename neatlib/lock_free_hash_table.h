//
// Created by jiahua on 2019/5/12.
//

#ifndef NEATLIB_LOCK_FREE_HASH_TABLE_H
#define NEATLIB_LOCK_FREE_HASH_TABLE_H

#include <epoch/memory_epoch.h>

namespace neatlib {

template<class Key, class T, class Hash = std::hash <Key>,
        std::size_t HASH_LEVEL = DEFAULT_NEATLIB_HASH_LEVEL,
        std::size_t ROOT_HASH_LEVEL = DEFAULT_NEATLIB_HASH_LEVEL>
class LockFreeHashTable {
private:
    struct node {

    };

public:
private:
    epoch::MemoryEpoch<node> epoch_;
};

}


#endif //NEATLIB_LOCK_FREE_HASH_TABLE_H
