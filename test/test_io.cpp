//
// Created by evan on 5/3/19.
//

#include <catch2/catch.hpp>

#include "../lib/io.hpp"
#include "../src/types.hpp"
#include "../src/batch_manager.hpp"

TEST_CASE("single file read and written correctly" "[InterleavedIOScheduler]") {
    InterleavedIOScheduler<Read, BucketManager<Read, std::string, DummyCache<Read, std::string>> > io;


}