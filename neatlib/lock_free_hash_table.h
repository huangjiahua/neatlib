//
// Created by jiahua on 2019/5/12.
//

#ifndef NEATLIB_LOCK_FREE_HASH_TABLE_H
#define NEATLIB_LOCK_FREE_HASH_TABLE_H

#include <epoch/memory_epoch.h>
#include <array>
#include <queue>
#include <vector>
#include <cassert>
#include <memory>
#include "util.h"
#include "../util/NodeQueue.h"

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

    using insert_type = std::integral_constant<int, 0>;
    using get_type = std::integral_constant<int, 1>;
    using update_type = std::integral_constant<int, 2>;
    using remove_type = std::integral_constant<int, 3>;

private:
    enum class NodeType {
        Data, Array, Plain
    };

    struct DataBlock {
        Key key;
        T mapped;
        size_t hash;

        DataBlock(const Key &k, const T &m) : key(k), mapped(m), hash(Hash()(k)) {}

        inline void init(const Key &k, const T &m) {
            key = k;
            mapped = m;
            hash = Hash()(k);
        }
    };

    struct Node {
        NodeType type;

        explicit Node(NodeType nodeType) : type(nodeType) {}

        virtual ~Node() = default;
    };

    struct DataNode : Node {
        DataBlock data;
        DataNode *next = nullptr;
        DataNode *last = nullptr;
        int64_t epoch = -1;

        DataNode(const Key &k, const T &m) : Node(NodeType::Data), data(k, m) {}

        ~DataNode() override = default;
    };

    struct ArrayNode : Node {
        std::array<std::atomic<Node *>, kArraySize> arr;

        ArrayNode() : Node(NodeType::Array) {
            for (std::atomic<Node *> &ptr : arr)
                ptr.store(nullptr);
        }

        ~ArrayNode() override = default;
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


        inline static DataNode *NewDataNode(LockFreeHashTable &ht, const Key &key, const T &mapped) {
            uint32_t tid = FASTER::core::Thread::id();
            assert(tid <= ht.data_pool_.size());
            DataNodeQueue &queue = ht.data_pool_[tid];
            if (!queue.Empty()) {
                int64_t epoch = std::numeric_limits<int64_t>::max();
                if (queue.Top()->epoch > 0)
                    epoch = ht.epoch_.inner_epoch_.ComputeNewSafeToReclaimEpoch(ht.epoch_.inner_epoch_.current_epoch);
                if (queue.Top()->epoch <= epoch) {
                    DataNode *ptr = queue.Pop();
                    ptr->type = NodeType::Data;
                    ptr->data.init(key, mapped);
                    return ptr;
                }
            }
            return new DataNode(key, mapped);
        }


        struct DataNodeDeleter {
            LockFreeHashTable *ht = nullptr;

            inline void operator()(Node *node) const {
                uint32_t tid = FASTER::core::Thread::id();
                assert(tid <= ht->data_pool_.size());
                static_cast<DataNode *>(node)->epoch = -1;
                ht->data_pool_[tid].Push(static_cast<DataNode *>(node));
            }

            explicit DataNodeDeleter(LockFreeHashTable *h) : ht(h) {}
        };


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

        using DataNodePtr = std::unique_ptr<Node, DataNodeDeleter>;
        using ArrayNodePtr = std::unique_ptr<ArrayNode>;

        void InsertOrUpdate(LockFreeHashTable &ht, const Key &key,
                            const T *mapped_ptr, size_t hash, bool insert) {
            pos = nullptr;
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
                } else {
                    curr_hash = root_hash(hash);
                    assert(curr_hash < kRootArraySize);
                    atomic_pos = &ht.root_[curr_hash];
                }
                pos = atomic_pos->load();
                size_t fail = 0;
                for (; fail < kFailLimit; fail++) {
                    if (pos == nullptr) {
                        if (!insert) {
                            return;
                        }
                        DataNodePtr tmp_ptr(NewDataNode(ht, key, *mapped_ptr), DataNodeDeleter(&ht));
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
                            DataNodePtr tmp_ptr(nullptr, DataNodeDeleter(&ht));
                            if (mapped_ptr != nullptr) tmp_ptr.reset(NewDataNode(ht, key, *mapped_ptr));
                            if (atomic_pos->compare_exchange_strong(pos, tmp_ptr.get())) {
                                pos = old;
                                tmp_ptr.release();
                                assert(pos->type == NodeType::Data);
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
                            ArrayNodePtr tmp_arr_ptr(new ArrayNode);
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
                        assert(pos != nullptr || pos->type == NodeType::Array);
                        curr_arr_ptr = static_cast<ArrayNode *>(pos);
                        break;
                    }

                }
                if (fail == kFailLimit) {
                    pos = nullptr;
                    return;
                }
            }
            assert(insert);
        }

        Locator(LockFreeHashTable &ht, const Key &key, const T &mapped, size_t hash, insert_type) {
            // insert data and set pos to the newly inserted pointer on the data structure
            InsertOrUpdate(ht, key, &mapped, hash, true);
        }

        Locator(LockFreeHashTable &ht, const Key &key, size_t hash, get_type) {
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

        Locator(LockFreeHashTable &ht, const Key &key, const T &mapped, size_t hash, update_type) {
            // update the data and set pos to the old pointer which is removed from the data structure
            InsertOrUpdate(ht, key, &mapped, hash, false);
        }

        Locator(LockFreeHashTable &ht, const Key &key, size_t hash, remove_type) {
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
    explicit LockFreeHashTable(size_t expectedThreadCount, size_t expectedDataNum = 1000000) :
            epoch_(expectedThreadCount),
            data_pool_(expectedThreadCount + 2) {
        for (std::atomic<Node *> &ptr : root_)
            ptr.store(nullptr);
        size_t m = 1, num = kArraySize, level = 1;
        size_t total_bit = sizeof(Key) * 8;
        if (total_bit <= 64) {
            for (size_t i = 0; i < total_bit; i++)
                m *= 2;
            for (; num < m; num += num * kArraySize)
                level++;
            level++;
        }
        maxLevel_ = 20;
        maxElement_ = m;

        size_t each = expectedDataNum / expectedThreadCount;
        for (uint32_t i = 0; i < expectedThreadCount; i++) {
            for (size_t j = 0; j < each; j++) {
                data_pool_[i].Push((DataNode *) malloc(sizeof(DataNode)));
            }
        }
    }

    ~LockFreeHashTable() {
        epoch_.EnterEpoch();
        for (std::atomic<Node *> &ptr : root_)
            RecursiveDestroyNode(ptr.load(std::memory_order_relaxed));
    }

    inline bool Insert(const Key &key, const T &mapped) {
        Locator locator(*this, key, mapped, Hash()(key), insert_type());
        DataNode *tmp = static_cast<DataNode *>(locator.pos);
//        assert(locator.pos == nullptr || key == locator.GetKey() && mapped == locator.GetMapped());
        return locator.pos != nullptr;
    }

    inline std::pair<const Key, T> Get(const Key &key) {
        epoch_.EnterEpoch();
        Locator locator(*this, key, Hash()(key), get_type());
        if (locator.pos == nullptr) {
            epoch_.EnterEpoch();
            throw std::out_of_range("No element found");
        }
        DataNode *dataNode = static_cast<DataNode *>(locator.pos);
        std::pair<const Key, T> ret{dataNode->data.key, dataNode->data.mapped};
        epoch_.LeaveEpoch();
        return ret;
    }

    inline bool Update(const Key &key, const T &newMapped) {
        Locator locator(*this, key, newMapped, Hash()(key), update_type());
        assert(locator.pos == nullptr || locator.pos->type == NodeType::Data);
        if (locator.pos == nullptr) {
            epoch_.EnterEpoch();
            return false;
        }
        uint64_t epoch = epoch_.inner_epoch_.BumpCurrentEpoch();
        DataNode *old = static_cast<DataNode*>(locator.pos);
        old->epoch = epoch;
        size_t tid = FASTER::core::Thread::id();
        data_pool_[tid].Push(old);
//        epoch_.BumpEpoch(static_cast<Node *>(locator.pos), data_pool_);
        return true;
    }

    inline bool Remove(const Key &key) {
        Locator locator(*this, key, Hash()(key), remove_type());
        if (locator.pos == nullptr) {
            epoch_.EnterEpoch();
            return false;
        }
        assert(locator.GetKey() == key);
        uint64_t epoch = epoch_.inner_epoch_.BumpCurrentEpoch();
        DataNode *old = static_cast<DataNode*>(locator.pos);
        old->epoch = epoch;
        size_t tid = FASTER::core::Thread::id();
        data_pool_[tid].Push(old);
//        epoch_.BumpEpoch(locator.pos, data_pool_);
        return true;
    }

private:
    using DataNodeQueue = NodeQueue<DataNode>;
    std::vector<DataNodeQueue> data_pool_;
    epoch::MemoryEpoch<Node, std::vector<DataNodeQueue>, DataNode> epoch_;
    std::array<std::atomic<Node *>, kRootArraySize> root_;
    size_t maxLevel_;
    size_t maxElement_;
};

}


#endif //NEATLIB_LOCK_FREE_HASH_TABLE_H
