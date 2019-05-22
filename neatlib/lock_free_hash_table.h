//
// Created by jiahua on 2019/5/12.
//

#ifndef NEATLIB_LOCK_FREE_HASH_TABLE_H
#define NEATLIB_LOCK_FREE_HASH_TABLE_H

#include <epoch/memory_epoch.h>
#include <array>
#include <cassert>
#include <memory>
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
    static constexpr size_t kFailLimit = 20;
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
            delete arrNodePtr;
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

        void InsertOrUpdate(LockFreeHashTable &ht, const Key &key, const T *mapped_ptr, size_t hash, bool insert) {
            size_t level = 0;
            bool end = false;
            ArrayNode *curr_arr_ptr = nullptr;

            for (; level < ht.maxLevel_ && !end; level++) {
                size_t curr_hash = 0;
                std::atomic<Node *> *atomic_pos = nullptr;
                if (level > 0) {
                    curr_hash = level_hash(hash, level);
                    assert(curr_hash < kArraySize);
                    assert(curr_arr_ptr != nullptr);
                    atomic_pos = &curr_arr_ptr->arr[curr_hash];
                    pos = atomic_pos->load();
                } else {
                    curr_hash = root_hash(hash);
                    assert(curr_hash < kRootArraySize);
                    atomic_pos = &ht.root_[curr_hash];
                    pos = atomic_pos->load();
                }

                size_t fail = 0;
                for (; fail < kFailLimit; fail++) {
                    if (pos == nullptr) {
                        if (!insert) {
                            return;
                        }
                        std::unique_ptr<Node> tmp_ptr(new DataNode(key, *mapped_ptr));
                        if (atomic_pos->compare_exchange_strong(pos, tmp_ptr.get())) {
                            pos = tmp_ptr.release();
                            end = true;
                            break;
                        } else {
                            continue;
                        }
                    } else if (pos->type == NodeType::Data) {
                        if (!insert) { // for update and remove
                            Node *old = pos;
                            std::unique_ptr<Node> tmp_ptr(nullptr);
                            if (mapped_ptr != nullptr) tmp_ptr.reset(new DataNode(key, *mapped_ptr));
                            if (atomic_pos->compare_exchange_strong(pos, tmp_ptr.get())) {
                                pos = old;
                                tmp_ptr.release();
                                return;
                            } else {
                                continue;
                            }
                        } else { // for insert
                            if (static_cast<DataNode *>(pos)->data.hash == hash) {
                                pos = nullptr;
                                end = true;
                                break;
                            }
                            std::unique_ptr<ArrayNode> tmp_arr_ptr(new ArrayNode);
                            size_t next_level_hash = level_hash(
                                    static_cast<DataNode *>(pos)->data.hash,
                                    level + 1
                            );
                            tmp_arr_ptr->arr[next_level_hash].store(pos);
                            if (atomic_pos->compare_exchange_strong(pos, tmp_arr_ptr.get())) {
                                curr_arr_ptr = tmp_arr_ptr.release();
                                break;
                            } else {
                                continue;
                            }
                        }
                    } else {
                        assert(pos != nullptr && pos->type == NodeType::Array);
                        curr_arr_ptr = static_cast<ArrayNode *>(pos);
                    }
                    if (fail == kFailLimit) pos = nullptr;
                }
            }
        }

        Locator(LockFreeHashTable &ht, const Key &key, const T &mapped, size_t hash) {
            // insert data and set pos to the newly inserted pointer on the data structure
            InsertOrUpdate(ht, key, &mapped, hash, true);
        }

        Locator(LockFreeHashTable &ht, const Key &key, size_t hash) {
            // find the data and set pos to the pointer of the data
            size_t level = 0;
            ArrayNode *curr_arr_ptr = nullptr;

            for (; level < ht.maxLevel_; level++) {
                size_t curr_hash = 0;
                if (level > 0) {
                    curr_hash = level_hash(hash, level);
                    assert(curr_hash < kArraySize);
                    pos = curr_arr_ptr->arr[curr_hash].load();
                } else {
                    curr_hash = root_hash(hash);
                    assert(curr_hash <= kRootArraySize);
                    pos = ht.root_[curr_hash].load();
                }

                if (pos == nullptr) {
                    break;
                } else if (pos->type == NodeType::Data) {
                    if (hash != static_cast<DataNode *>(pos)->data.hash)
                        pos = nullptr;
                    break;
                } else {
                    assert(pos->type == NodeType::Array);
                    curr_arr_ptr = static_cast<ArrayNode *>(pos);
                }
            }
        }

        Locator(LockFreeHashTable &ht, const Key &key, const T &mapped, size_t hash, std::true_type) {
            // update the data and set pos to the old pointer which is removed from the data structure
            InsertOrUpdate(ht, key, &mapped, hash, false);
        }

        explicit Locator(LockFreeHashTable &ht, Key &key, size_t hash, std::true_type, std::true_type) {
            // remove the data pointer on the data structure and set pos to the pointer
            InsertOrUpdate(ht, key, nullptr, hash, false);
        }

        const Key &GetKey() const {
            assert(pos != nullptr);
            return static_cast<DataNode *>(pos)->data.key;
        }

        const Key &GetMapped() const {
            assert(pos != nullptr);
            return static_cast<DataNode *>(pos)->data.mapped;
        }
    };

private:

public:
    LockFreeHashTable(size_t expectedThreadCount = 4) :
            epoch_(expectedThreadCount + 1) {
        for (std::atomic<Node *> &ptr : root_)
            ptr.store(nullptr);
        size_t m = 1, num = kArraySize, level = 1;
        size_t total_bit = sizeof(Key) * 8;
        if (total_bit < 64) {
            for (size_t i = 0; i < total_bit; i++)
                m *= 2;
            for (; num < m; num += num * kArraySize)
                level++;
            level++;
        }
        maxLevel_ = level;
        maxElement_ = m;
    }

    ~LockFreeHashTable() {
        epoch_.UpdateEpoch();
        for (std::atomic<Node *> &ptr : root_)
            RecursiveDestroyNode(ptr.load(std::memory_order_relaxed));
    }

    inline bool Insert(const Key &key, const T &mapped) {
        Locator locator(*this, key, mapped, Hash()(key));
        assert(key == locator.GetKey() && mapped == locator.GetMapped());
        epoch_.UpdateEpoch();
        return locator.pos != nullptr;
    }

    inline std::pair<const Key, T> Get(const Key &key) {
        Locator locator(*this, key, Hash()(key));
        if (locator.pos == nullptr) {
            epoch_.UpdateEpoch();
            throw std::out_of_range("No element found");
        }
        DataNode *dataNode = static_cast<DataNode *>(locator.pos);
        std::pair<const Key, T> ret{dataNode->data.key, dataNode->data.mapped};
        epoch_.UpdateEpoch();
        return ret;
    }

    inline bool Update(const Key &key, const T &newMapped) {
        Locator locator(*this, key, newMapped, Hash()(key), std::true_type());
        if (locator.pos == nullptr) {
            epoch_.UpdateEpoch();
            return false;
        }
        epoch_.BumpEpoch(static_cast<Node*>(locator.pos));
        epoch_.UpdateEpoch();
        return true;
    }

    inline bool Remove(const Key &key) {
        Locator locator(*this, key, Hash()(key), std::true_type(), std::true_type());
        if (locator.pos == nullptr) {
            epoch_.UpdateEpoch();
            return false;
        }
        epoch_.BumpEpoch(locator.pos);
        epoch_.UpdateEpoch();
        return true;
    }

private:
    epoch::MemoryEpoch<Node> epoch_;
    std::array<std::atomic<Node *>, kRootArraySize> root_;
    size_t maxLevel_;
    size_t maxElement_;
};

}


#endif //NEATLIB_LOCK_FREE_HASH_TABLE_H
