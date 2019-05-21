//
// Created by jiahua on 2019/5/12.
//

#ifndef NEATLIB_LOCK_FREE_HASH_TABLE_H
#define NEATLIB_LOCK_FREE_HASH_TABLE_H

#include <epoch/memory_epoch.h>
#include <array>
#include <cassert>
#include "util.h"

namespace neatlib {

template<class Key, class T, class Hash = std::hash<Key>,
        std::size_t HASH_LEVEL = DEFAULT_NEATLIB_HASH_LEVEL,
        std::size_t ROOT_HASH_LEVEL = DEFAULT_NEATLIB_HASH_LEVEL>
class LockFreeHashTable {
private:
    static constexpr size_t kArraySize =
            static_cast<const std::size_t>(get_power2<HASH_LEVEL>::value);
    static constexpr size_t kRootArraySize =
            static_cast<const std::size_t>(get_power2<ROOT_HASH_LEVEL>::value);
private:
    enum class NodeType {
        Data, Array, Plain
    };

    struct DataBlock {
        Key key;
        T mapped;
        size_t hash;

        DataBlock(const Key &k, const T &m) : key(k), mapped(m), hash(Hash()(k)) {}
    };

    struct Node {
        NodeType type;

        explicit Node(NodeType nodeType) : type(nodeType) {}
    };

    struct DataNode : Node {
        DataBlock data;

        DataNode(const Key &k, const T &m) : Node(NodeType::Data), data(k, m) {}
    };

    struct ArrayNode : Node {
        std::array<std::atomic<Node *>, kArraySize> arr;

        ArrayNode() : Node(NodeType::Array) {
            for (std::atomic<Node *> &ptr : arr)
                ptr.store(nullptr);
        }
    };

    static void RecursiveDestroyNode(Node *nodePtr) {
        if (nodePtr == nullptr) return;
        else if (nodePtr->type == NodeType::Data) delete nodePtr;
        else {
            assert(nodePtr->type == NodeType::Array);
            ArrayNode *arrNodePtr = static_cast<ArrayNode *>(nodePtr);
            for (std::atomic<Node *> &ptr : arrNodePtr->arr)
                RecursiveDestroyNode(ptr.load(std::memory_order_relaxed));
        }
    }

    struct Locator {
        Node *pos = nullptr;

        static inline size_t root_hash(std::size_t hash) {
            // get the hash fragment to address the root_
            return hash & (kRootArraySize - 1);
        }

        static inline size_t level_hash(std::size_t hash, std::size_t level) {
            // get the hash fragment to address the level th level array
            hash >>= ROOT_HASH_LEVEL;
            level--;
            return util::level_hash<Key>(hash, level, kArraySize, HASH_LEVEL);
        }

        Locator(const Key &key, const T &mapped, size_t hash) {
            // insert data and set pos to the newly inserted pointer on the data structure
            // TODO
        }

        Locator(const Key &key, size_t hash) {
            // find the data and set pos to the pointer of the data
        }

        Locator(const Key &key, const T &mapped, size_t hash, std::true_type) {
            // update the data and set pos to the old pointer which is removed from the data structure
        }

        Locator(const Key &key, size_t hash, std::true_type) {
            // remove the data pointer on the data structure and set pos to the pointer
        }
    };

private:

public:
    LockFreeHashTable(size_t expectedThreadCount = 4) :
            epoch_(expectedThreadCount + 1) {
        for (std::atomic<Node *> &ptr : root_)
            ptr.store(nullptr);
    }

    ~LockFreeHashTable() {
        epoch_.UpdateEpoch();
        for (std::atomic<Node *> &ptr : root_)
            RecursiveDestroyNode(ptr.load(std::memory_order_relaxed));
    }

    inline bool Insert(const Key &key, const T &mapped) {
        Locator locator(key, mapped, Hash()(key));
        epoch_.UpdateEpoch();
        return locator.pos != nullptr;
    }

    inline std::pair<const Key, T> Get(const Key &key) {
        Locator locator(key, Hash()(key));
        if (locator.pos == nullptr)
            throw std::out_of_range("No element found");
        DataNode *dataNode = static_cast<DataNode *>(locator.pos);
        epoch_.UpdateEpoch();
        return {dataNode->data.key, dataNode->data.mapped};
    }

    inline bool Update(const Key &key, const T &newMapped) {
        Locator locator(key, newMapped, Hash()(key), std::true_type());
        if (locator.pos == nullptr) return false;
        epoch_.BumpEpoch(locator.pos);
        epoch_.UpdateEpoch();
        return true;
    }

    inline bool Remove(const Key &key) {
        Locator locator(key, Hash()(key), std::true_type());
        if (locator.pos == nullptr) return false;
        epoch_.BumpEpoch(locator.pos);
        epoch_.UpdateEpoch();
        return true;
    }

private:
    epoch::MemoryEpoch<Node> epoch_;
    std::array<std::atomic<Node *>, kRootArraySize> root_;
};

}


#endif //NEATLIB_LOCK_FREE_HASH_TABLE_H
