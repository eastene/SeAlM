//
// Created by evan on 5/1/19.
//

#include <future>
#include <chrono>

#include <Catch2/catch.hpp>

#include "../src/types.hpp"
#include "../lib/storage.hpp"

TEST_CASE("single bucket created and consumed correctly" "[batch_buckets]") {
    Read r(4, "");
    BufferedBatchBuckets<Read> bb;

    r[0] = "@test";
    r[1] = "AAGGC";
    r[2] = "+";
    r[3] = "=====";

    for (int i = 1; i < 50000; i++) {
        bb.insert(r);
    }

    REQUIRE(bb.size() == 49999);
    REQUIRE(bb.num_buckets() == 0);

    SECTION("filling buffer adds single new bucket") {
        bb.insert(r);
        REQUIRE(bb.size() == 50000);
        REQUIRE(bb.num_buckets() == 1);
    }

    SECTION("requesting bucket that exists returns unique pointer to bucket and does not timeout") {
        bb.insert(r);
        std::atomic_bool cancel = false;
        std::future<std::unique_ptr<std::vector<Read> > > future = std::async(std::launch::async,
                                                                              [&]() { return bb.wait_for_bucket(cancel); });
        std::future_status status;
        status = future.wait_for(std::chrono::seconds(1));
        REQUIRE(status == std::future_status::ready);
        REQUIRE(future.get()->size() == 50000);
        REQUIRE(bb.size() == 0);
    }

    SECTION("requesting bucket will timeout when no bucket available") {
        std::atomic_bool cancel = false;
        std::future<std::unique_ptr<std::vector<Read> > > future = std::async(std::launch::async,
                                                                              [&]() { return bb.wait_for_bucket(cancel); });
        std::future_status status;
        status = future.wait_for(std::chrono::milliseconds(50));
        cancel = true;
        REQUIRE(status == std::future_status::timeout);
        REQUIRE(future.get() == nullptr);
    }

    SECTION("call to next_bucket_async returns a bucket or times out if none available") {
        bb.insert(r);
        auto bucket = bb.next_bucket_async(std::chrono::milliseconds(500));
        REQUIRE(bucket->size() == 50000);

        bucket = bb.next_bucket_async(std::chrono::milliseconds(500));
        REQUIRE(bucket == nullptr);
    }

    SECTION("returned bucket contains expected values") {
        bb.insert(r);
        auto bucket = bb.next_bucket_async(std::chrono::milliseconds(500));
        uint32_t matches = 0;
        for (const auto &read : *bucket) {
            if (read[0] == r[0] && read[1] == r[1] && read[2] == r[2] && read[3] == r[3])
                matches++;
        }
        REQUIRE(matches == bucket->size());
    }
}

TEST_CASE("multiple buckets produced and consumed correctly" "[batch_buckets]") {
    Read r(4, "");
    BufferedBatchBuckets<Read> bb;

    r[0] = "@test";
    r[1] = "AAGGC";
    r[2] = "+";
    r[3] = "=====";

    for (int i = 0; i < 100000; i++) {
        bb.insert(r);
    }

    REQUIRE(bb.size() == 100000);
    REQUIRE(bb.num_buckets() == 2);

    SECTION("buckets will be consumed from first chain until empty"){
        std::string old_label = r[0];
        std::string old_seq = r[1];

        r[0] = "@test1";
        r[1] = "CCAGC";
        r[2] = "+";
        r[3] = "=====";

        for (int i = 0; i < 50000; i++) {
            bb.insert(r);
        }

        REQUIRE(bb.num_buckets() == 3);

        auto bucket = bb.next_bucket_async(std::chrono::milliseconds(50));
        REQUIRE((*bucket)[0][0] == old_label);
        REQUIRE((*bucket)[0][1] == old_seq);
        bucket = bb.next_bucket_async(std::chrono::milliseconds(50));
        REQUIRE((*bucket)[0][0] == old_label);
        REQUIRE((*bucket)[0][1] == old_seq);
        bucket = bb.next_bucket_async(std::chrono::milliseconds(50));
        REQUIRE((*bucket)[0][0] == r[0]);
        REQUIRE((*bucket)[0][1] == r[1]);
    };

    SECTION("longest bucket chain will be consumed after first"){
        r[0] = "@test1";
        r[1] = "CCAGC";
        r[2] = "+";
        r[3] = "=====";

        std::string old_label = r[0];
        std::string old_seq = r[1];

        for (int i = 0; i < 100000; i++) {
            bb.insert(r);
        }

        r[0] = "@test2";
        r[1] = "TTAGC";
        r[2] = "+";
        r[3] = "=====";

        for (int i = 0; i < 50000; i++) {
            bb.insert(r);
        }

        REQUIRE(bb.num_buckets() == 5);

        auto bucket = bb.next_bucket_async(std::chrono::milliseconds(50));
        bucket = bb.next_bucket_async(std::chrono::milliseconds(50));
        bucket = bb.next_bucket_async(std::chrono::milliseconds(50));
        REQUIRE((*bucket)[0][0] == old_label);
        REQUIRE((*bucket)[0][1] == old_seq);
        bucket = bb.next_bucket_async(std::chrono::milliseconds(50));
        REQUIRE((*bucket)[0][0] == old_label);
        REQUIRE((*bucket)[0][1] == old_seq);
        bucket = bb.next_bucket_async(std::chrono::milliseconds(50));
        REQUIRE((*bucket)[0][0] == r[0]);
        REQUIRE((*bucket)[0][1] == r[1]);
    };
}