
add_executable(basic_ht_test basic_ht_test.cpp)
if (MAKE_UNIQUE_NOT_SUPPORT)
    target_compile_definitions(basic_ht_test PUBLIC MAKE_UNIQUE_NOT_SUPPORT)
endif()

add_executable(conc_ht_test conc_ht_test.cpp)
if (UNIX)
    target_link_libraries(conc_ht_test pthread)
endif()

if (UNIX AND NOT APPLE)
    list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_LIST_DIR}")
    include(FindFolly)
    include_directories(${FOLLY_INCLUDE_DIR})
    message("FOLLY LOAD SUCCEED")
    add_executable(lockfree_test conc_ht_test2.cpp ../neatlib/concurrent_hash_table.h)
    target_link_libraries(lockfree_test ${FOLLY_LIBRARIES} pthread glog)
endif()
