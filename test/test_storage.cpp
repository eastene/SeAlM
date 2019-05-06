//
// Created by evan on 5/1/19.
//

#include <future>
#include <chrono>

#include <catch2/catch.hpp>

#include "../src/types.hpp"
#include "../lib/storage.hpp"

TEST_CASE("single bucket created and consumed correctly" "[BufferedBuckets]") {
    Read r(4, "");
    BufferedBuckets<std::pair<uint64_t, Read>> bb;

    r[0] = "@test";
    r[1] = "AAGGC";
    r[2] = "+";
    r[3] = "=====";

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

    r[0] = "@test";
    r[1] = "AAGGC";
    r[2] = "+";
    r[3] = "=====";

    for (int i = 0; i < 50000; i++) {
        bb.insert(std::make_pair(i, r));
    }

    REQUIRE(bb.size() == 50000);
    REQUIRE(bb.num_buckets() == 1   );

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
}