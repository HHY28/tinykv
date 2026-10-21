cmake_minimum_required(VERSION 3.20)
project(tinykv LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)

add_executable(kv_server
    src/main.cpp
    src/command.cpp
    src/thread_pool.cpp
    src/store.cpp
    src/raft.cpp
    src/server.cpp
)
target_include_directories(kv_server PRIVATE include)
target_link_libraries(kv_server PRIVATE Threads::Threads)
target_compile_options(kv_server PRIVATE -Wall -Wextra -Wpedantic)
