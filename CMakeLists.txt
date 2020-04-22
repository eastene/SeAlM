cmake_minimum_required(VERSION 3.10)
project(SeAlM)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "-Wall -Wextra")
set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")
set(CMAKE_CXX_FLAGS_RELEASE "-pthread")

# standard packages
find_package(Threads)

# external packages
include(ExternalProject)

set(EXTERNAL_INSTALL_LOCATION ${CMAKE_BINARY_DIR}/external)

ExternalProject_Add(Catch2
        GIT_REPOSITORY https://github.com/catchorg/Catch2
        GIT_TAG v2.11.3
        CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${EXTERNAL_INSTALL_LOCATION}
        )

ExternalProject_Add(cpp-subprocess
        GIT_REPOSITORY https://github.com/arun11299/cpp-subprocess
        GIT_TAG v2.0
        CONFIGURE_COMMAND ""
        BUILD_COMMAND     ""
        INSTALL_COMMAND   ""
        SOURCE_DIR ${EXTERNAL_INSTALL_LOCATION}/include/cpp-subprocess
        )

include_directories(${EXTERNAL_INSTALL_LOCATION}/include)
link_directories(${EXTERNAL_INSTALL_LOCATION}/lib)

add_executable(SeAlM src/main.cpp src/wrapped_mapper.cpp src/wrapped_mapper.hpp lib/cache.hpp src/mapping_utils.hpp lib/pipeline.hpp lib/types.hpp lib/storage.hpp lib/io.hpp lib/config.hpp lib/logging.hpp src/prep_experiment.hpp lib/signaling.hpp lib/process.h)
add_executable(test_SeAlM test/test_main.cpp test/test_storage.cpp lib/storage.hpp test/test_cache.cpp lib/io.hpp lib/logging.hpp test/test_io.cpp test/test_pipeline.cpp test/test_config.cpp)

add_dependencies(SeAlM cpp-subprocess)
target_link_libraries(SeAlM ${CMAKE_THREAD_LIBS_INIT} stdc++fs)
add_dependencies(test_SeAlM Catch2 cpp-subprocess)
target_link_libraries(test_SeAlM ${CMAKE_THREAD_LIBS_INIT} stdc++fs)

#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libstdc++")