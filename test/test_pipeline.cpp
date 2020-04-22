//
// Created by Evan Stene on 2019-05-04.
//

#include <catch2/catch.hpp>

#include "../lib/pipeline.hpp"
#include "../lib/types.hpp"

TEST_CASE("single file read and written correctly" "[BucketedPipelineManager]") {
    BucketedPipelineManager<Read, const char*, const char*> pipeline;


}