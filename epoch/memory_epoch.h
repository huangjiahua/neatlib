//
// Created by jiahua on 2019/5/12.
//

#ifndef NEATLIB_EPOCH_H
#define NEATLIB_EPOCH_H
#include <memory>
#include "faster/light_epoch.h"

namespace neatlib {
namespace epoch {

template <typename T>
class Delete_Context : public FASTER::core::IAsyncContext {
public:
    Delete_Context(T* ptr_): ptr(ptr_) { }
    Delete_Context(const Delete_Context& other): ptr(other.ptr) { }
protected:
    FASTER::core::Status DeepCopy_Internal(FASTER::core::IAsyncContext*& context_copy) final {
        return FASTER::core::IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }
public:
    T* ptr;
};

template <typename T, class Deleter = std::default_delete<T> >
class MemoryEpoch {
private:
    FASTER::core::LightEpoch inner_epoch_;

    static auto delete_callback = [](FASTER::core::IAsyncContext* ctxt) {
        FASTER::core::CallbackContext<Delete_Context<T>> context(ctxt);
        Deleter()(context->ptr);
    };

public:
    MemoryEpoch(size_t max_thread_cnt) : inner_epoch_(max_thread_cnt) {}

    inline uint64_t UpdateEpoch() {
        return inner_epoch_.ProtectAndDrain();
    }

    inline bool IsInEpoch() {
        return inner_epoch_.IsProtected();
    }

    inline void LeaveEpoch() {
        inner_epoch_.Unprotect();
    }

    uint64_t BumpEpoch(T* ptr) {
        Delete_Context<T> context(ptr);
        FASTER::core::IAsyncContext* context_copy;
        context.DeepCopy(context_copy);
        return inner_epoch_.BumpCurrentEpoch(delete_callback, context_copy);
    }
};


} // namespace epoch
} // namespace neatlib

#endif //NEATLIB_EPOCH_H
