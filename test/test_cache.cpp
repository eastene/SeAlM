//
// Created by evan on 5/2/19.
//

#include <catch2/catch.hpp>

#include "../lib/cache.hpp"

TEST_CASE("dummy cache initializes correctly" "[DummyCache]") {
    DummyCache<std::string, std::string> cache;
    std::string key = "test_key";
    std::string value = "test_value";

    REQUIRE(cache.hits() == 0);
    REQUIRE(cache.hit_rate() == 0);
    REQUIRE(cache.capacity() == 900000);

    SECTION("dummy cache stores no values") {
        cache.insert(key, value);
        REQUIRE(cache.size() == 0);
        REQUIRE(cache.find(key) == cache.end());
        REQUIRE(cache[key] != value);
        REQUIRE(cache.hits() == 0);
    }
}

TEST_CASE("lru cache initializes and evicts correctly" "[LRUCache]") {
    LRUCache<std::string, std::string> cache;
    std::string key = "test_key";
    std::string value = "test_value";

    REQUIRE(cache.hits() == 0);
    REQUIRE(cache.hit_rate() == 0);
    REQUIRE(cache.capacity() == 900000);

    SECTION("lru cache stores single value correctly") {
        cache.insert(key, value);
        REQUIRE(cache.size() == 1);
        REQUIRE(cache.find(key) != cache.end());
        REQUIRE(cache[key] == value);
        REQUIRE(cache.hits() == 1);
    }

    SECTION("lru cache stores multiple values correctly") {
        std::string old_key = key;
        cache.insert(key, value);
        key = "test_key2";
        value = "test_value2";
        cache.insert(key, value);
        cache.find(key);
        cache.find(old_key);
        REQUIRE(cache.hits() == 2);
    }

    SECTION("lru cache evicts proper key-value pair") {
        std::string first_key = key;
        cache.insert(key, value);
        for (uint32_t i = 0; i < cache.capacity(); i++) {
            key = "test_key" + std::to_string(i + 2);
            value = "test_value" + std::to_string(i + 2);
            cache.insert(key, value);
        }
        REQUIRE(cache.size() == cache.capacity());
        REQUIRE(cache.find(first_key) == cache.end());
    }
}

TEST_CASE("mru cache evicts correctly" "[MRUCache]") {
    MRUCache<std::string, std::string> cache;
    std::string key = "test_key";
    std::string value = "test_value";

    SECTION("mru cache evicts proper key-value pair") {
        std::string first_key = key;
        cache.insert(key, value);
        for (uint32_t i = 0; i < cache.capacity(); i++) {
            key = "test_key" + std::to_string(i + 2);
            value = "test_value" + std::to_string(i + 2);
            cache.insert(key, value);
        }
        std::string last_key = "test_key" + std::to_string(cache.capacity());
        REQUIRE(cache.size() == cache.capacity());
        REQUIRE(cache.find(first_key) != cache.end());
        REQUIRE(cache.find(last_key) == cache.end());
    }
}