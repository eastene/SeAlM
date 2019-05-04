//
// Created by Evan Stene on 2019-05-04.
//

#include <catch2/catch.hpp>

#include "../lib/pipeline.hpp"
#include "../src/types.hpp"

TEST_CASE("single file read and written correctly" "[BucketedPipelineManager]") {
    //BucketedPipelineManager<Read, std::string, DummyCache<Read, std::string>>
    BucketedPipelineManager<Read, std::string, RedupeRef, InMemCache<std::string, std::string>> pipeline;
}