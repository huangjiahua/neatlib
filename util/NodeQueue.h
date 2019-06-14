//
// Created by jiahua on 2019/6/5.
//

#ifndef NODELIST_NODEQUEUE_H
#define NODELIST_NODEQUEUE_H
#include <cstdlib>

template <typename NODE>
class NodeQueue {
public:
    NodeQueue(): head_(nullptr), tail_(nullptr) {}

    ~NodeQueue() {
        NODE *curr = head_;
        while (!Empty()) {
            NODE *del = Pop();
            free(del);
        }
    }

    bool Empty() const {
        return head_ == nullptr;
    }

    void Push(NODE *node) {
        node->next = head_;
        node->last = nullptr;
        if (!Empty())
            head_->last = node;
        else {
            tail_ = node;
            tail_->next = nullptr;
        }
        head_ = node;
    }

    NODE *Pop() {
        NODE *ret = tail_;
        tail_ = tail_->last;
        if (tail_ == nullptr)
            head_ = nullptr;
        else
            tail_->next = nullptr;
        return ret;
    }
private:
    NODE *head_;
    NODE *tail_;
};

#endif //NODELIST_NODEQUEUE_H
