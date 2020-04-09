//
// Created by evan on 5/3/19.
//

#include <catch2/catch.hpp>
#include <experimental/filesystem>

#include "../lib/io.hpp"
#include "../lib/types.hpp"

class PrefixHasher : public DataHasher< std::pair<uint64_t, Read> >{
public:

    uint64_t _hash_fn(const std::pair<uint64_t, Read> &data) {
        auto payload = data.second;
        switch (payload[1][0]) {
            case 'A':
                return 0;
            case 'C':
                return 1;
            case 'T':
                return 2;
            case 'G':
                return 3;
            default:
                return 3;
        }
    }

    uint64_t _required_table_width(){ return 4; }
};

TEST_CASE("single file read and written correctly" "[InterleavedIOScheduler]") {
    std::unique_ptr< OrderedSequenceStorage< std::pair<uint64_t, Read> > > bb;
    bb = std::make_unique< BufferedBuckets< std::pair<uint64_t, Read> > >();
    std::shared_ptr<DataHasher< std::pair<uint64_t, Read> > > p = std::make_shared<PrefixHasher>();
    bb->set_data_properties(p);
    bb->set_bucket_size(50000);
    bb->set_num_buckets(4);

    InterleavedIOScheduler<Read> io;
    io.set_input_pattern("[a-z]+\\.txt");
    io.set_storage_subsystem(bb);

    std::ofstream fout("a.txt");

    SECTION("a bucket will be created after reading sufficient data") {
        io.from_dir(std::experimental::filesystem::current_path());
        REQUIRE(io.size() == 1);

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
        REQUIRE(io.size() == 0);
        REQUIRE(io.empty());
    }

    std::remove("a.txt");
}