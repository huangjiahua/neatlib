#ifndef NEATLIB_UTIL_H
#define NEATLIB_UTIL_H

#ifdef MAKE_UNIQUE_NOT_SUPPORT

namespace std {
    template<class T> struct _Unique_if {
        typedef unique_ptr<T> _Single_object;
    };

    template<class T> struct _Unique_if<T[]> {
        typedef unique_ptr<T[]> _Unknown_bound;
    };

    template<class T, size_t N> struct _Unique_if<T[N]> {
        typedef void _Known_bound;
    };

    template<class T, class... Args>
        typename _Unique_if<T>::_Single_object
        make_unique(Args&&... args) {
            return unique_ptr<T>(new T(std::forward<Args>(args)...));
        }

    template<class T>
        typename _Unique_if<T>::_Unknown_bound
        make_unique(size_t n) {
            typedef typename remove_extent<T>::type U;
            return unique_ptr<T>(new U[n]());
        }

    template<class T, class... Args>
        typename _Unique_if<T>::_Known_bound
        make_unique(Args&&...) = delete;
}

#endif // MAKE_UNIQUE_NOT_SUPPORT

namespace neatlib {

constexpr std::size_t DEFAULT_NEATLIB_HASH_LEVEL = 4;

template<std::size_t B>
struct get_power2 {
    static constexpr int value = 2 * get_power2<B - 1>::value;
};

template<>
struct get_power2<0> {
    static constexpr int value = 1;
};

namespace util {

template<typename Key>
inline std::size_t level_hash(const std::size_t hash, const std::size_t level,
                              const std::size_t arr_size, const std::size_t total_level) {
    std::size_t mask = (arr_size - 1);
    std::size_t ret = (hash >> (total_level * level)) & mask;
    assert(ret < arr_size);
    return ret;
}

} // namespace util

}

#endif // NEATLIB_UTIL_H