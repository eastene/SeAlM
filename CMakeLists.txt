cmake_minimum_required(VERSION 3.14)
project(aligner_cache)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "-Wall -Wextra")
set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")
set(CMAKE_CXX_FLAGS_RELEASE "-pthread")

find_package(Threads)

#find_package(Boost 1.65.1 COMPONENTS system REQUIRED)
#
#if(Boost_FOUND)
#
#    message(STATUS "Boost_INCLUDE_DIRS: ${Boost_INCLUDE_DIRS}")
#    message(STATUS "Boost_LIBRARIES: ${Boost_LIBRARIES}")
#    message(STATUS "Boost_VERSION: ${Boost_VERSION}")
#
#    include_directories(${Boost_INCLUDE_DIRS})
#
#endif()

add_executable(aligner_cache src/main.cpp src/wrapped_mapper.cpp src/wrapped_mapper.hpp lib/in_mem_cache.hpp src/utils.hpp src/batch_manager.cpp src/batch_manager.hpp src/types.hpp lib/batch_buckets.hpp)
add_executable(test_aligner_cache test/test_main.cpp test/test_batch_buckets.cpp lib/batch_buckets.hpp test/test_in_mem_cache.cpp)
#if(Boost_FOUND)
#
#    target_link_libraries(aligner_cache ${Boost_LIBRARIES})
#
#endif()
target_link_libraries (aligner_cache ${CMAKE_THREAD_LIBS_INIT} stdc++fs)
target_link_libraries (test_aligner_cache ${CMAKE_THREAD_LIBS_INIT} stdc++fs)