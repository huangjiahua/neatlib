
add_executable(basic_ht_test basic_ht_test.cpp)
if (MAKE_UNIQUE_NOT_SUPPORT)
    target_compile_definitions(basic_ht_test PUBLIC MAKE_UNIQUE_NOT_SUPPORT)
endif()

add_executable(conc_ht_test conc_ht_test.cpp)
if (UNIX)
    target_link_libraries(conc_ht_test pthread)
endif()
