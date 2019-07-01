//
// Created by jiahua on 2019/5/12.
//

#ifndef NEATLIB_EPOCH_H
#define NEATLIB_EPOCH_H

#include <memory>
#include "faster/light_epoch.h"



namespace neatlib {
namespace epoch {

template<typename T, typename Pool>
class Delete_Context : public FASTER::core::IAsyncContext {
public:
    Delete_Context(T *ptr_, Pool *pool_) : ptr(ptr_),  pool(pool_) {}

    Delete_Context(const Delete_Context &other) : ptr(other.ptr), pool(other.pool) {}

protected:
    FASTER::core::Status DeepCopy_Internal(FASTER::core::IAsyncContext *&context_copy) final {
        return FASTER::core::IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

public:
    T *ptr;
    Pool *pool;
};

template<typename T, typename Pool, typename Org>
class MemoryEpoch {
public:
    FASTER::core::LightEpoch inner_epoch_;

    static void delete_callback(FASTER::core::IAsyncContext *ctxt) {
        FASTER::core::CallbackContext<Delete_Context<T, Pool>> context(ctxt);
        uint32_t tid = FASTER::core::Thread::id();
        assert(tid <= context->pool->size());
        auto &queue = context->pool->operator[](tid);
        queue.Push(static_cast<Org*>(context->ptr));
    }

public:
    explicit MemoryEpoch(size_t max_thread_cnt) : inner_epoch_(max_thread_cnt) {}

    inline uint64_t EnterEpoch() {
        return inner_epoch_.ReentrantProtect();
    }

    inline bool IsInEpoch() {
        return inner_epoch_.IsProtected();
    }

    inline void LeaveEpoch() {
        inner_epoch_.ReentrantUnprotect();
    }

    uint64_t BumpEpoch(T *ptr, Pool &pool) {
        Delete_Context<T, Pool> context(ptr, &pool);
        FASTER::core::IAsyncContext *context_copy;
        context.DeepCopy(context_copy);
        return inner_epoch_.BumpCurrentEpoch(delete_callback, context_copy);
    }
};


} // namespace epoch
} // namespace neatlib

#endif //NEATLIB_EPOCH_H
