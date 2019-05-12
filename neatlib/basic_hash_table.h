//
// Created by jiahua on 2019/3/12.
//

#ifndef NEATLIB_BASIC_HASH_TABLE_H
#define NEATLIB_BASIC_HASH_TABLE_H

#include <cassert>
#include <memory>
#include <cstddef>
#include <array>
#include <stack>
#include <cmath>
#include <limits>
#include <stdexcept>
#include <functional>
#include "util.h"

namespace neatlib {

template<class Key, class T, class Hash = std::hash<Key>,
        class KeyEqual = std::equal_to<Key>,
        class Allocator = std::allocator<std::pair<const Key, T>>,
        std::size_t HASH_LEVEL = DEFAULT_NEATLIB_HASH_LEVEL>
class BasicHashTable {
private:
    using node_type = bool;

    constexpr static bool ARRAY_NODE = true;
    constexpr static bool DATA_NODE = false;

    constexpr static std::size_t ARRAY_SIZE =
            static_cast<const size_t>(HASH_LEVEL > 10 ? 65536 : get_power2<HASH_LEVEL>::value);

public:
    using key_type = Key;
    using mapped_type = T;
    using value_type = std::pair<const Key, T>;
    using difference_type = std::ptrdiff_t;
    using hasher = Hash;
    using key_equal = KeyEqual;
    using allocator_type = Allocator;

private:
    struct node {
        node_type type_;

        node(node_type type) : type_(type) {}
    };

    struct data_node : node {
        std::pair<const Key, T> data_;

        data_node(const Key &key, const T &mapped) :
                node(DATA_NODE), data_(key, mapped) {}

        std::size_t hash() {
            return Hash()(data_.first);
        }
    };

    using node_ptr = std::unique_ptr<node>;

    struct array_node : node {
        std::array<std::unique_ptr<node>, ARRAY_SIZE> arr_;

        array_node() : node(ARRAY_NODE),
                       arr_() {}

        constexpr static std::size_t size() { return ARRAY_SIZE; }
    };

    struct array_node_pool {
        std::stack<std::unique_ptr<array_node>> stack_;

        std::unique_ptr<array_node> get() {
            std::unique_ptr<node> n = std::move(stack_.front());
            stack_.pop();
            return std::move(n);
        }

        array_node *get_ptr() {
            auto ret = stack_.top().release();
            stack_.pop();
            return ret;
        }

        void put() { stack_.push(std::make_unique<array_node>()); }

        void put(std::size_t count) {
            for (std::size_t i = 0; i < count; i++) put();
        }
    };

    struct locator {
        std::unique_ptr<node> *loc_ref_ = nullptr;

        std::size_t level_hash(std::size_t hash, std::size_t level) {
//            std::size_t mask = (ARRAY_SIZE - 1);
//            std::size_t ret = (hash >> (sizeof(Key) * 8 - HASH_LEVEL * level) ) & mask ;
//            assert(ret < ARRAY_SIZE);
//            return ret;
            return util::level_hash<Key>(hash, level, ARRAY_SIZE, HASH_LEVEL);
        }

        const Key &key() {
            return static_cast<data_node *>(loc_ref_->get())->data_.first;
        }

        const std::pair<const Key, T> &value() {
            return static_cast<data_node *>(loc_ref_->get())->data_;
        }

        // for finding only
        explicit locator(BasicHashTable &ht, const Key &key) {
            std::size_t hash = ht.hasher_(key);
            std::size_t level = 0;
            // this will not go out this scope
            array_node *curr_arr_ptr = &ht.root_node_;

            for (; level < ht.max_level_; level++) {
                std::size_t curr_hash = level_hash(hash, level);
                std::unique_ptr<node> &node_ptr_ref = curr_arr_ptr->arr_[curr_hash];
                if (node_ptr_ref == nullptr) {
                    loc_ref_ = nullptr;
                    break;
                } else if (node_ptr_ref->type_ == DATA_NODE) {
                    if (hash == static_cast<data_node *>(node_ptr_ref.get())->hash())
                        loc_ref_ = &node_ptr_ref;
                    else
                        loc_ref_ = nullptr;
                    break;
                } else {
                    assert(node_ptr_ref->type_ == ARRAY_NODE);
                    curr_arr_ptr = static_cast<array_node *>(node_ptr_ref.get());
                }
            }
        }

        // for insertion only
        locator(BasicHashTable &ht, const Key &key, const T &mapped) {
            std::size_t hash = ht.hasher_(key);
            std::size_t level = 0;
            // this will not go out this scope
            array_node *curr_arr_ptr = &ht.root_node_;

            for (; level < ht.max_level_; level++) {
                std::size_t curr_hash = level_hash(hash, level);
                assert(curr_hash <= ARRAY_SIZE);
                std::unique_ptr<node> &node_ptr_ref = curr_arr_ptr->arr_[curr_hash];

                // this is the place to Insert
                if (node_ptr_ref == nullptr) {
                    node_ptr_ref = std::make_unique<data_node>(key, mapped);
                    loc_ref_ = &node_ptr_ref;
                    break;
                } else if (node_ptr_ref->type_ == DATA_NODE) {
                    // this will not go out this scope
                    data_node *temp_data_ptr = static_cast<data_node *>(node_ptr_ref.get());

                    // first, we should judge whether this is the same key with the key to be inserted
                    if (temp_data_ptr->hash() == hash) {
                        // the key is already there, to Update the user should use Update rather than Insert
                        loc_ref_ = nullptr;
                        break;
                    }

                    temp_data_ptr = static_cast<data_node *>(node_ptr_ref.release());
                    if (ht.pool_.stack_.empty()) {
                        node_ptr_ref = std::make_unique<array_node>();
                    } else {
                        node_ptr_ref.reset(ht.pool_.get_ptr());
                    }

                    // this will not go out this scope
                    auto temp_arr_ptr = static_cast<array_node *>(node_ptr_ref.get());

                    std::size_t temp_hash = level_hash(temp_data_ptr->hash(), level + 1);
                    temp_arr_ptr->arr_[temp_hash].reset(temp_data_ptr);

                    curr_arr_ptr = temp_arr_ptr;
                    continue;
                } else {
                    assert(node_ptr_ref->type_ == ARRAY_NODE);
                    curr_arr_ptr = static_cast<array_node *>(node_ptr_ref.get());
                    continue;
                }
            }
        }
    };

public:
    BasicHashTable() {
        std::size_t m = 1, num = ARRAY_SIZE, level = 1;
        std::size_t total_bit = sizeof(Key) * 8;
        if (total_bit < 64) {
            for (std::size_t i = 0; i < total_bit; i++)
                m *= 2;
            for (; num < m; num += num * ARRAY_SIZE)
                level++;
        } else {
            m = std::numeric_limits<std::size_t>::max();
            auto m2 = m / 2;
            for (; num < m2; num += num * ARRAY_SIZE)
                level++;
            level++;
        }

        max_level_ = level;
        max_ = m;
    };

    explicit BasicHashTable(std::size_t capacity) : BasicHashTable() {
        Reserve(capacity);
    }

    void Reserve(std::size_t new_cap) {
        std::size_t new_arr = 0;

        if (new_cap % ARRAY_SIZE) new_arr = new_cap / ARRAY_SIZE;
        else new_arr = new_cap / ARRAY_SIZE + 1;

        pool_.put(new_arr);
    }

    bool Insert(const Key &key, const T &mapped) {
        locator locator_(*this, key, mapped);
        if (locator_.loc_ref_ == nullptr)
            return false;
        assert(key_equal_(locator_.key(), key));
        assert(key_equal_(locator_.value().second, mapped));
        ++size_;
        return true;
    }

    std::pair<const Key, T> Get(const Key &key) {
        locator locator_(*this, key);
        if (locator_.loc_ref_ == nullptr)
            throw std::out_of_range("No Element Found");
        assert(key_equal_(locator_.key(), key));
        return locator_.value();
    }

    std::shared_ptr<std::pair<const Key, T>> Find(const Key &key) {
        locator locator_(*this, key);
        if (locator_.loc_ref_ == nullptr)
            return nullptr;
        assert(key_equal_(locator_.key(), key));
        return std::make_shared<std::pair<const Key, T>>(locator_.value());
    }

    bool Remove(const Key &key) {
        locator locator_(*this, key);
        if (locator_.loc_ref_ == nullptr)
            return false;
        locator_.loc_ref_->reset(nullptr);
        --size_;
        return true;
    }

    bool Update(const Key &key, const T &new_mapped) {
        locator locator_(*this, key);
        if (locator_.loc_ref_ == nullptr)
            return false;
        auto dn = static_cast<data_node *>(locator_.loc_ref_->get());
        dn->data_.second = new_mapped;
        return true;
    }

    std::size_t Size() const {
        return size_;
    }


private:
    array_node root_node_;
    array_node_pool pool_;
    KeyEqual key_equal_;
    Hash hasher_;
    std::size_t size_ = 0;
    std::size_t max_level_ = 0;
    std::size_t max_ = 0;

};

} // namespace neatlib

#endif //NEATLIB_BASIC_HASH_TABLE_H
