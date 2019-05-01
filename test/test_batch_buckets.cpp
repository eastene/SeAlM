//
// Created by evan on 5/1/19.
//

#include "catch.hpp"
#include "../src/types.hpp"
#include "../lib/batch_buckets.hpp"

TEST_CASE( "single bucket created correctly" "[batch_buckets]") {
    BatchBuckets<Read> bb;

    Read r;
    r[0] = "@test";
    r[1] = "AAGGC";
    r[2] = "+";
    r[3] = "=====";

    for(int i = 0; i < 50000; i++){
        bb.insert(r);
    }

    REQUIRE(bb.size() == 50000);

}