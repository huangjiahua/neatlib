cmake_minimum_required(VERSION 3.12)
project(neatlib)

set(CMAKE_CXX_STANDARD 14)

find_package(Boost 1.45.0)

include_directories( ${Boost_INCLUDE_DIRS} )

add_subdirectory(./neatlib)
add_subdirectory(./test)

add_executable(neatlib1 main.cpp neatlib/concurrent_hast_table.h)
