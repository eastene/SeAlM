//
// Created by evan on 5/3/19.
//

#include <catch2/catch.hpp>
#include <experimental/filesystem>

#include "../lib/io.hpp"
#include "../src/types.hpp"
//#include "../src/batch_manager.hpp"

TEST_CASE("single file read and written correctly" "[InterleavedIOScheduler]") {
    //BucketManager<Read, std::string, DummyCache<Read, std::string>>
    InterleavedIOScheduler<Read, std::string> io;

    io.set_input_pattern("[a-z]+\\.txt");
    std::ofstream fout("a.txt");
    io.from_dir(std::experimental::filesystem::current_path());

    REQUIRE(io.size() == 1);

    SECTION("a bucket will be created after reading sufficient data") {
        for (int i = 0; i < 50000; i++) {
            fout << "@test";
            fout << "AAGGC";
            fout << "+";
            fout << "=====";
        }

        io.begin_reading();
        std::future<std::vector<Read> > future = std::async(std::launch::async,
                                                            [&]() { return io.request_bucket(); });
        std::future_status status;
        status = future.wait_for(std::chrono::milliseconds(50));
        REQUIRE(future.get().size() == 50000);
        REQUIRE(future.get()[0][0] == "@test");
    }

    std::remove("a.txt");
}