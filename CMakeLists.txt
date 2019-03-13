cmake_minimum_required(VERSION 3.12)
project(neatlib)

set(CMAKE_CXX_STANDARD 14)

add_subdirectory(./neatlib)
add_subdirectory(./test)

add_executable(neatlib main.cpp)