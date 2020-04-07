//
// Created by evan on 5/3/19.
//

#include <catch2/catch.hpp>
#include <experimental/filesystem>

#include "../lib/io.hpp"
#include "../lib/types.hpp"

TEST_CASE("single file read and written correctly" "[InterleavedIOScheduler]") {
    InterleavedIOScheduler<Read> io;

    io.set_input_pattern("[a-z]+\\.txt");
    std::ofstream fout("a.txt");
    io.from_dir(std::experimental::filesystem::current_path());

    REQUIRE(io.size() == 1);

    SECTION("a bucket will be created after reading sufficient data") {
        for (int i = 0; i < 50000; i++) {
            fout << "@test\n";
            fout << "AAGGC\n";
            fout << "+\n";
            fout << "=====\n";
            fout.flush();
        }

        io.begin_reading();
        auto future = std::async(std::launch::async, [&]() { return io.request_bucket(); });
        std::future_status status;
        status = future.wait_for(std::chrono::milliseconds(500));

        auto bucket = std::move(future.get());
        REQUIRE(bucket->size() == 50000);
        REQUIRE((*bucket)[0].second[0] == "@test");
        REQUIRE(io.empty());
        REQUIRE(io.size() == 0);
    }

    std::remove("a.txt");
}