cmake_minimum_required(VERSION 3.14)
project(aligner_cache)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "-Wall -Wextra")
set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")
set(CMAKE_CXX_FLAGS_RELEASE "-pthread")

find_package (Threads)

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

add_executable(aligner_cache main.cpp wrapped_mapper.cpp wrapped_mapper.hpp in_mem_cache.cpp in_mem_cache.hpp utils.hpp batch_manager.cpp batch_manager.hpp types.hpp)

#if(Boost_FOUND)
#
#    target_link_libraries(aligner_cache ${Boost_LIBRARIES})
#
#endif()
target_link_libraries (aligner_cache ${CMAKE_THREAD_LIBS_INIT})