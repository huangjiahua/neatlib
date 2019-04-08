//
// Created by jiahua on 2019/3/16.
//

#ifndef NEATLIB_CONCURRENT_HAST_TABLE_H
#define NEATLIB_CONCURRENT_HAST_TABLE_H
#include <boost/smart_ptr/shared_ptr.hpp>
#include <boost/smart_ptr/make_shared.hpp>
#include <boost/smart_ptr/atomic_shared_ptr.hpp>
#include <boost/lockfree/stack.hpp>
#include <atomic>
#include <memory>
#include <array>
#include <limits>
#include <type_traits>
#include <cassert>
#include "util.h"

namespace neatlib {

constexpr int FAIL_LIMIT = 20;

template<class Key, class T, class Hash = std::hash<Key>,
        class KeyEqual = std::equal_to<Key>,
        class Allocator = std::allocator<std::pair<const Key, T>>,
        std::size_t HASH_LEVEL = DEFAULT_NEATLIB_HASH_LEVEL,
        std::size_t ROOT_HASH_LEVEL = DEFAULT_NEATLIB_HASH_LEVEL>
class concurrent_hash_table {
private:
    enum class node_type {
        DATA_NODE = 0, ARRAY_NODE
    };

    constexpr static std::size_t ARRAY_SIZE =
            static_cast<const std::size_t>(get_power2<HASH_LEVEL>::value);
    constexpr static std::size_t ROOT_ARRAY_SIZE =
            static_cast<const std::size_t>(get_power2<ROOT_HASH_LEVEL>::value);

    using insert_type = std::integral_constant<int, 0>;
    using modify_type = std::integral_constant<int, 1>;

public:
    using key_type = Key;
    using mapped_type = T;
    using value_type = std::pair<const Key, const T>;
    using difference_type = std::ptrdiff_t;
    using hasher = Hash;
    using key_equal = KeyEqual;
    using allocator_type = Allocator;

private:
    struct node {
        node_type type_;
        explicit node(node_type type): type_(type) {}
    };

    struct data_node: node {
        const std::pair<const Key, const T> data_;
        const std::size_t hash_;

        data_node(const Key &key, const T &mapped):
            node(node_type::DATA_NODE), data_(key, mapped), hash_(Hash()(key)) { }

        data_node(const Key &key, const T &mapped, const std::size_t h):
            node(node_type::DATA_NODE), data_(key, mapped), hash_(h) { }

        std::size_t hash() const { return hash_; }
    };

    struct array_node: node {
        std::array<boost::atomic_shared_ptr<node>, ARRAY_SIZE> arr_;
        array_node(): node(node_type::ARRAY_NODE), arr_() {}
        constexpr static std::size_t size() { return ARRAY_SIZE; }
    };

    struct reserved_pool {
        boost::lockfree::stack<boost::shared_ptr<data_node>> data_st_;

        reserved_pool(): data_st_() {}
        void put() {
            data_st_.push(boost::make_shared<data_node>());
        }
        void put(std::size_t sz) {
            for (std::size_t i = 0; i < sz; i++) put();
        }
        boost::shared_ptr<data_node> get_data_node(const Key &key, const T &mapped, std::size_t hash) {
            boost::shared_ptr<data_node> p(nullptr);
            data_st_.pop(p);
            p->data_.first = key;
            p->data_.second = mapped;
            p->hash_ = hash;
            return std::move(p);
        }
    };

    struct locator {
        boost::shared_ptr<node> loc_ref_ = nullptr;

        static std::size_t level_hash(std::size_t hash, std::size_t level) {
//            std::size_t mask = (ARRAY_SIZE - 1);
//            std::size_t ret = (hash >> (sizeof(Key) * 8 - HASH_LEVEL * level)) & mask;
//            assert(ret < ARRAY_SIZE);
//            return ret;
            hash >>= ROOT_HASH_LEVEL;
            level--;
            return util::level_hash<Key>(hash, level, ARRAY_SIZE, HASH_LEVEL);
        }

        static std::size_t root_hash(std::size_t hash) {
            return hash & (ROOT_ARRAY_SIZE - 1);
        }

        const Key &key() {
            return static_cast<data_node*>(loc_ref_.get())->data_.first;
        }

        const std::pair<const Key, const T> &value() {
//            return loc_ref_->data_;
            return static_cast<data_node*>(loc_ref_.get())->data_;
        }

        // for finding only
        locator(concurrent_hash_table &ht, const Key &key) {
            std::size_t hash = ht.hasher_(key);
            std::size_t level = 0;
            array_node *curr_arr_ptr = nullptr;

            for(; level < ht.max_level_; level++) {
                std::size_t curr_hash = 0;
                if (level) {
                    curr_hash = level_hash(hash, level);
                    assert(curr_hash <= ARRAY_SIZE);
                    loc_ref_ = curr_arr_ptr->arr_[curr_hash].load();
                }
                else {
                    curr_hash = root_hash(hash);
                    assert(curr_hash <= ROOT_ARRAY_SIZE);
                    loc_ref_ = ht.root_arr_[curr_hash].load();
                }

                if (!loc_ref_.get()) {
                    break;
                } else if (loc_ref_.get()->type_ == node_type::DATA_NODE) {
                    if (hash != static_cast<data_node*>(loc_ref_.get())->hash())
                        loc_ref_ = nullptr;
                    break;
                } else {
                    assert(loc_ref_->type_ == node_type::ARRAY_NODE);
                    curr_arr_ptr = static_cast<array_node*>(loc_ref_.get());
                }
            }
        }

        locator(concurrent_hash_table &ht, const Key &key, const T *mappedp, modify_type) {
            bool remove_flag = false;
            if (!mappedp)
                remove_flag = true;

            std::size_t hash = ht.hasher_(key);
            std::size_t level = 0;
            bool end = false;
            array_node *curr_arr_ptr = nullptr;
        }

        // for insertion only
        locator(concurrent_hash_table &ht, const Key &key, const T &mapped, insert_type) {
            std::size_t hash = ht.hasher_(key);
            std::size_t level = 0;
            bool end = false;
            array_node *curr_arr_ptr = nullptr;

            for (; level < ht.max_level_ && !end; level++) {
                std::size_t curr_hash = 0;
                boost::atomic_shared_ptr<node> *atomic_pos = nullptr;
                if (level) {
                    curr_hash = level_hash(hash, level);
                    assert(curr_hash <= ARRAY_SIZE);
                    atomic_pos = &curr_arr_ptr->arr_[curr_hash];
                    loc_ref_ = atomic_pos->load();
                }
                else {
                    curr_hash = root_hash(hash);
                    assert(curr_hash <= ROOT_ARRAY_SIZE);
                    atomic_pos = &ht.root_arr_[curr_hash];
                    loc_ref_ = atomic_pos->load();
                }
                int fail = 0;

                for ( ; fail < FAIL_LIMIT; fail++) {
                    if (!loc_ref_.get()) {
                        boost::shared_ptr<node> tmp_ptr(boost::static_pointer_cast<node>(
                                boost::make_shared<data_node>(key, mapped, hash)));
                        if (atomic_pos->compare_exchange_strong(loc_ref_,
                                tmp_ptr)) {
                            // CAS succeeds means a successful insertion
                            loc_ref_ = std::move(tmp_ptr);
                            end = true;
                            break;
                        } else {
                            // CAS fails means the atomic ptr just got changed
//                            assert(loc_ref_->type_ == node_type::ARRAY_NODE ||
//                                   loc_ref_->type_ == node_type::DATA_NODE);
                            continue;
                        }
                    } else if (loc_ref_.get()->type_ == node_type::DATA_NODE) {
                        // first we should test if this is a duplicate key
                        if (static_cast<data_node*>(loc_ref_.get())->hash() == hash) {
                            // then insert fail, if user wants to update, update member function should be used
                            loc_ref_ = nullptr;
                            end = true;
                            break;
                        }
                        boost::shared_ptr<array_node> tmp_arr_ptr = boost::make_shared<array_node>();
                        std::size_t next_level_hash = level_hash(
                                static_cast<data_node*>(loc_ref_.get())->hash(),
                                level + 1
                                );
                        tmp_arr_ptr.get()->arr_[next_level_hash].store(loc_ref_);
                        if (atomic_pos->compare_exchange_strong(loc_ref_,
                                boost::static_pointer_cast<node>(tmp_arr_ptr))) {
                            // CAS succeeds means to change this atomic to array_node
                            curr_arr_ptr = tmp_arr_ptr.get();
                            break;
                        } else {
                            // CAS fails means the atomic was changed by other threads
//                            assert(loc_ref_ == nullptr ||
//                                   loc_ref_->type_ == node_type::DATA_NODE ||
//                                   loc_ref_->type_ == node_type::ARRAY_NODE);
                            continue;
                        }
                    } else {
//                        assert(loc_ref_->type_ == node_type::ARRAY_NODE);
                        curr_arr_ptr = static_cast<array_node*>(loc_ref_.get());
                    }
                }
                if (fail == FAIL_LIMIT) loc_ref_ = nullptr;
            }
        }
    };

public:
    concurrent_hash_table(): size_(0) {
        std::size_t m = 1, num = ARRAY_SIZE, level = 1;
        std::size_t total_bit = sizeof(Key) * 8;
        if (total_bit < 64) {
            for (std::size_t i = 0; i < total_bit; i++)
                m *= 2;
            for ( ; num < m; num += num * ARRAY_SIZE)
                level++;
        }
        else {
            m = std::numeric_limits<std::size_t>::max();
            auto m2 = m / 2;
            for ( ; num < m2; num += num * ARRAY_SIZE)
                level++;
            level++;
        }
        max_level_ = level;
        max_ = m;
    }
    concurrent_hash_table(std::size_t reserved): concurrent_hash_table() {
//        pool_.put(reserved);
    }

    ~concurrent_hash_table() noexcept = default;

    bool insert(const Key &key, const T &mapped) {
        locator locator_(*this, key, mapped, insert_type());
        if (locator_.loc_ref_ == nullptr)
            return false;
        assert(key_equal_(locator_.key(), key));
        assert(key_equal_(locator_.value().second, mapped));
//        size_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    std::pair<const Key, const T> get(const Key &key) {
        locator locator_(*this, key);
        if (locator_.loc_ref_ == nullptr)
            throw std::out_of_range("No Element Found");
        assert(key_equal_(locator_.key(), key));
        return locator_.value();
    }

    bool update(const Key &key, const T &new_mapped) {
        locator locator_(*this, key, &new_mapped, modify_type());
        if (locator_.loc_ref_ == nullptr)
            return false;
        assert(key_equal_(locator_.key(), key));
        assert(key_equal_(locator_.value().second, new_mapped));
        return true;
    }

    bool remove(const Key &key) {
        // TODO
        locator locator_(*this, key, nullptr, modify_type());
        if (locator_.loc_ref_ == nullptr)
            return false;
        return true;
    }

    std::size_t size() const {
        return size_.load();
    }

private:
    std::array<boost::atomic_shared_ptr<node>, ROOT_ARRAY_SIZE> root_arr_;
    KeyEqual key_equal_;
    Hash hasher_;
//    reserved_pool pool_;
    std::atomic<std::size_t> size_;
    std::size_t max_level_ = 0;
    std::size_t max_ = 0;
};

} // namespace neatlib


#endif //NEATLIB_CONCURRENT_HAST_TABLE_H