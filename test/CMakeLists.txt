
#SET(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} -mcx16 -latomic")
add_executable(basic_ht_test basic_ht_test.cpp)
if (MAKE_UNIQUE_NOT_SUPPORT)
    target_compile_definitions(basic_ht_test PUBLIC MAKE_UNIQUE_NOT_SUPPORT)
endif()

add_executable(conc_ht_test conc_ht_test.cpp)
if (UNIX)
    target_link_libraries(conc_ht_test pthread)
endif()

#if (UNIX AND NOT APPLE)
#    list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}")
#    include(FindFolly)
#    find_package(FOLLY)
#    if (FOLLY_FOUND)
#        include_directories(${FOLLY_INCLUDE_DIR})
#        message("FOLLY LOAD SUCCEED")
#        add_executable(lockfree_test conc_ht_test2.cpp ../neatlib/concurrent_hash_table.h)
#        target_link_libraries(lockfree_test ${FOLLY_LIBRARIES} pthread glog)
#    endif()
#endif()

set(EBR ../epoch/faster/lss_allocator.cc ../epoch/faster/thread.cc)

#add_executable(ebr_test EBR_test.cpp ../epoch/faster/lss_allocator.cc ../epoch/faster/thread.cc)

add_executable(ebr_ht_test ebr_ht_test.cpp ${EBR})
add_executable(ebr_ht_performance_test ebr_ht_performance_test.cpp ${EBR})
if (UNIX)
    target_link_libraries(ebr_ht_performance_test pthread)
endif()

#add_executable(lockfree_test2 conc_ht_test3.cpp jss_atomic_shared_ptr.h)
#if (UNIX)
#    target_link_libraries(lockfree_test2 pthread)
#endif()
