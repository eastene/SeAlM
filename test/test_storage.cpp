//
// Created by evan on 5/1/19.
//

#include <future>
#include <chrono>

#include <catch2/catch.hpp>

#include "../lib/types.hpp"
#include "../lib/storage.hpp"

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

TEST_CASE("single bucket created and consumed correctly" "[BufferedBuckets]") {
    Read r(4, "");
    BufferedBuckets<std::pair<uint64_t, Read>> bb;
    std::shared_ptr<DataHasher< std::pair<uint64_t, Read> > > p = std::make_shared<PrefixHasher>();
    bb.set_data_properties(p);
    bb.set_bucket_size(50000);

    r[0] = "@test";
    r[1] = "AAGGC";
    r[2] = "+";
    r[3] = "=====";

    SECTION("property assesor works correctly"){
        Read r2 = r;
        REQUIRE(p->_hash_fn(std::make_pair(0,r2)) == 0);
        r2[1] = "CAGGT";
        REQUIRE(p->_hash_fn(std::make_pair(0,r2)) == 1);
        r2[1] = "TAGGT";
        REQUIRE(p->_hash_fn(std::make_pair(0,r2)) == 2);
        r2[1] = "GAGGT";
        REQUIRE(p->_hash_fn(std::make_pair(0,r2)) == 3);
        r2[1] = "NAGGT";
        REQUIRE(p->_hash_fn(std::make_pair(0,r2)) == 3);
    }

    for (int i = 1; i < 50000; i++) {
        bb.insert(std::make_pair(i, r));
    }

    REQUIRE(bb.size() == 49999);
    REQUIRE(bb.num_buckets() == 0);

    SECTION("filling buffer adds single new bucket") {
        bb.insert(std::make_pair(50000, r));
        REQUIRE(bb.size() == 50000);
        REQUIRE(bb.num_buckets() == 1);
    }

    SECTION("requesting bucket that exists returns unique pointer to bucket and does not timeout") {
        bb.insert(std::make_pair(50000, r));
        auto bucket = bb.next_bucket();
        REQUIRE(bucket->size() == 50000);
        REQUIRE(bb.size() == 0);
    }

    SECTION("requesting bucket will stall when no bucket available") {
        auto future = std::async(std::launch::async, [&]() { return bb.next_bucket(); });
        std::future_status status;
        status = future.wait_for(std::chrono::milliseconds(50));
        bb.kill();
        REQUIRE(status == std::future_status::timeout);
        REQUIRE(future.get() == nullptr);
        bb.recover();
    }

    SECTION("call to next_bucket_async returns a bucket or times out if none available") {
        bb.insert(std::make_pair(50000, r));
        auto future = bb.next_bucket_async();
        std::future_status status;
        status = future.wait_for(std::chrono::milliseconds(50));
        REQUIRE(future.get()->size() == 50000);

        future = bb.next_bucket_async();
        status = future.wait_for(std::chrono::milliseconds(50));
        bb.kill();
        REQUIRE(status == std::future_status::timeout);
        bb.recover();
    }

    SECTION("returned bucket contains expected values") {
        bb.insert(std::make_pair(50000, r));
        auto future = bb.next_bucket_async();
        std::future_status status;
        status = future.wait_for(std::chrono::milliseconds(50));
        uint32_t matches = 0;
        auto bucket = std::move(future.get());
        for (const auto &read_pair : *bucket) {
            auto read = read_pair.second;
            if (read[0] == r[0] && read[1] == r[1] && read[2] == r[2] && read[3] == r[3])
                matches++;
        }
        REQUIRE(matches == bucket->size());
    }

    SECTION("flushing buffer creates bucket from non-empty buffer") {
        bb.flush();
        REQUIRE(bb.num_buckets() == 1);
        auto future = bb.next_bucket_async();
        std::future_status status;
        status = future.wait_for(std::chrono::milliseconds(50));
        REQUIRE(future.get()->size() == 49999);
    }
}

TEST_CASE("multiple buckets produced and consumed correctly" "[BufferedBuckets]") {
    Read r(4, "");
    BufferedBuckets<std::pair<uint64_t, Read>> bb;
    std::shared_ptr<DataHasher< std::pair<uint64_t, Read> > > p = std::make_shared<PrefixHasher>();
    bb.set_data_properties(p);
    bb.set_bucket_size(50000);
    bb.set_num_buckets(4);

    r[0] = "@test";
    r[1] = "AAGGC";
    r[2] = "+";
    r[3] = "=====";

    for (int i = 0; i < 50000; i++) {
        bb.insert(std::make_pair(i, r));
    }

    REQUIRE(bb.size() == 50000);
    REQUIRE(bb.num_buckets() == 1);

    SECTION("buckets will be consumed from first chain until empty"){
        std::string old_label = r[0];
        std::string old_seq = r[1];

        r[0] = "@test1";
        r[1] = "CCAGC";
        r[2] = "+";
        r[3] = "=====";

        for (int i = 0; i < 50000; i++) {
            bb.insert(std::make_pair(i, r));
        }

        REQUIRE(bb.num_buckets() == 2);

        auto future = bb.next_bucket_async();
        std::future_status status;
        status = future.wait_for(std::chrono::milliseconds(50));
        auto bucket = std::move(future.get());
        REQUIRE((*bucket)[0].second[0] == old_label);
        REQUIRE((*bucket)[0].second[1] == old_seq);
        future = bb.next_bucket_async();
        status = future.wait_for(std::chrono::milliseconds(50));
        bucket = std::move(future.get());
        REQUIRE((*bucket)[0].second[0] == r[0]);
        REQUIRE((*bucket)[0].second[1] == r[1]);
    };

    SECTION("longest bucket chain will be consumed after first"){
        r[0] = "@test1";
        r[1] = "CCAGC";
        r[2] = "+";
        r[3] = "=====";

        std::string old_label = r[0];
        std::string old_seq = r[1];

        for (int i = 0; i < 100000; i++) {
            bb.insert(std::make_pair(i, r));
        }

        r[0] = "@test2";
        r[1] = "TTAGC";
        r[2] = "+";
        r[3] = "=====";

        for (int i = 0; i < 50000; i++) {
            bb.insert(std::make_pair(i, r));
        }

        REQUIRE(bb.num_buckets() == 4);

        auto future = bb.next_bucket_async();
        std::future_status status;
        status = future.wait_for(std::chrono::milliseconds(50));
        auto bucket = std::move(future.get());
        future = bb.next_bucket_async();
        status = future.wait_for(std::chrono::milliseconds(50));
        bucket = std::move(future.get());
        REQUIRE((*bucket)[0].second[0] == old_label);
        REQUIRE((*bucket)[0].second[1] == old_seq);
        future = bb.next_bucket_async();
        status = future.wait_for(std::chrono::milliseconds(50));
        bucket = std::move(future.get());
        REQUIRE((*bucket)[0].second[0] == old_label);
        REQUIRE((*bucket)[0].second[1] == old_seq);
        future = bb.next_bucket_async();
        status = future.wait_for(std::chrono::milliseconds(50));
        bucket = std::move(future.get());
        REQUIRE((*bucket)[0].second[0] == r[0]);
        REQUIRE((*bucket)[0].second[1] == r[1]);
    };

    SECTION("bucket hash function divides reads correctly"){
        Read r1 (4, "");
        r1[0] = "@test2";
        r1[1] = "TTAGC";
        r1[2] = "+";
        r1[3] = "=====";

        Read r2 (4, "");
        r2[0] = "@test3";
        r2[1] = "CCAGC";
        r2[2] = "+";
        r2[3] = "=====";

        for (int i = 0; i < 100000; i++) {
            r = i % 2 == 0 ? r1 : r2;
            bb.insert(std::make_pair(i, r));
        }

        auto bucket = bb.next_bucket();
        bool is_A = true;
        for (const auto & read : *bucket){
            if (read.second[1][0] != 'A'){
                is_A = false;
                break;
            }
        }
        REQUIRE(is_A);

        bucket = bb.next_bucket();
        bool is_C = true;
        for (const auto & read : *bucket){
            if (read.second[1][0] != 'C'){
                is_C = false;
                break;
            }
        }
        REQUIRE(is_C);

        bucket = bb.next_bucket();
        bool is_T = true;
        for (const auto & read : *bucket){
            if (read.second[1][0] != 'T'){
                is_T = false;
                break;
            }
        }
        REQUIRE(is_T);
    }
}