cmake_minimum_required(VERSION 3.14)
project(aligner_cache)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "-Wall -Wextra")
set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")
set(CMAKE_CXX_FLAGS_RELEASE "-pthread")

# standard packages
find_package(Threads)

# 3rd party packages with FetchContent
include(FetchContent)
FetchContent_Declare(
        cpp-subprocess
        GIT_REPOSITORY https://github.com/arun11299/cpp-subprocess.git
        GIT_TAG 871577770a437b2784d0c75888f8101b0b42de4d
)

FetchContent_Declare(
        catch2
        GIT_REPOSITORY https://github.com/catchorg/Catch2.git
        GIT_TAG 9c741fe96073ed620ffc032afbed1f3c789d2b68
)
FetchContent_MakeAvailable(cpp-subprocess Catch2)

add_executable(aligner_cache src/main.cpp src/wrapped_mapper.cpp src/wrapped_mapper.hpp lib/cache.hpp src/utils.hpp src/batch_manager.cpp src/batch_manager.hpp src/types.hpp lib/storage.hpp lib/io.hpp)
add_executable(test_aligner_cache test/test_main.cpp test/test_storage.cpp lib/storage.hpp test/test_cache.cpp lib/io.hpp test/test_io.cpp)

target_link_libraries(aligner_cache ${CMAKE_THREAD_LIBS_INIT} stdc++fs cpp-subprocess)
target_link_libraries(test_aligner_cache ${CMAKE_THREAD_LIBS_INIT} Catch2)
