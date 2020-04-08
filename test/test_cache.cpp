//
// Created by evan on 5/2/19.
//

#define CATCH_CONFIG_ENABLE_BENCHMARKING
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
    int cache_size = 1000;
    LRUCache<std::string, std::string> cache;
    cache.set_max_size(cache_size);

    std::string key = "test_key";
    std::string value = "test_value";

    REQUIRE(cache.hits() == 0);
    REQUIRE(cache.hit_rate() == 0);
    REQUIRE(cache.capacity() == cache_size);

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
    int cache_size = 1000;
    MRUCache<std::string, std::string> cache;
    cache.set_max_size(cache_size);

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

TEST_CASE("bloom filter cache behaves as expected", "[BFECache]"){
    int cache_size = 1000;
    std::string key = "ACGTN";
    std::string not_key = "TGCNA";

    BFECache<std::string, std::string> cache(65536, 2, key.size());
    std::shared_ptr<InMemCache<std::string, std::string> > c;
    c = std::make_shared<LRUCache<std::string, std::string> >();
    cache.set_cache(c);

    cache.set_max_size(cache_size);

    SECTION("bloom hash works correctly") {
        cache.add_key(key);
        REQUIRE(cache.possibly_exists(key));
        REQUIRE(!cache.possibly_exists(not_key));
    }

    cache.clear();
    cache.set_max_size(1);
    std::string value = "test";
    SECTION("bloom decorates cache correctly") {
        REQUIRE(cache.capacity() == 1);
        cache.insert(key, value);
        REQUIRE(cache.find(key) == cache.end());
        REQUIRE(cache.misses() == 1);
        cache.insert(key, value);
        REQUIRE(cache.find(key) != cache.end());
        REQUIRE(cache.at(key) == value);
        REQUIRE(cache.size() == 1);
        REQUIRE(cache.hits() == 1);

        cache.insert(not_key, value);
        REQUIRE(cache.find(key) != cache.end());
        REQUIRE(cache.at(key) == value);
        REQUIRE(cache.size() == 1);
        REQUIRE(cache.hits() == 2);

        cache.insert(not_key, value);
        REQUIRE(cache.find(key) == cache.end());
        REQUIRE(cache.misses() == 2);
        REQUIRE(cache.size() == 1);
    }

    cache.clear();
    cache.set_max_size(cache_size);
    std::string value1 = "test1";
    SECTION("multiple values insert to cache correctly") {
        cache.insert(key, value);
        cache.insert(not_key, value);
        REQUIRE(cache.size() == 0);
        cache.insert(key, value);
        cache.insert(not_key, value1);
        REQUIRE(cache.size() == 2);
        REQUIRE(cache.at(key) != value1);
        REQUIRE(cache.at(not_key) == value1);
    }
}

TEST_CASE("benchmark cache inserting with lru eviction" "[LRUCache]"){
    int cache_size = 10000;
    LRUCache<std::string, std::string> cache;
    cache.set_max_size(cache_size);
    std::string key = "test_key";
    std::string value = "test_value";

    BENCHMARK("Insert Until Full") {
        for (int i = 0; i < cache_size; i++) {
            cache.insert(key + std::to_string(i), value + std::to_string(i));
        }
    };

    BENCHMARK("Insert After Full") {
         for (int i = 0; i < cache_size; i++) {
             cache.insert(key + std::to_string(i), value + std::to_string(i));
         }
    };

    BENCHMARK("Insert After Full - No Evict and Trim") {
       for (int i = 0; i < cache_size; i++) {
           cache.insert_no_evict(key + std::to_string(i), value + std::to_string(i));
       }
       cache.trim();
    };

    cache.clear();
    key = "ACGCATCAGCTACAGGAGCTCCTAGAGCGCGAGCGAGCATTACACATTACTCATCTACTAGCAGCGACATCAGCAGCGACAGAGAGAGAGTTTAATTTAC";
    BENCHMARK("Insert long key, short value"){
        for (int i = 0; i < cache_size; i++) {
            cache.insert(key + std::to_string(i), value + std::to_string(i));
        }
    };

    value = "ACGCATCAGCTACAGGAGCTCCTAGAGCGCGAGCGAGCATTACACATTACTCATCTACTAGCAGCGACATCAGCAGCGACAGAGAGAGAGTTTAATTTAC ++#+#++$#$+#+#++#+##++#+#+@+#$+++@++!@+#+!+#++$%++^^+&+%^+$+%#+%+@+$@+#$+@+#$+!++++@#$_@#$_@))^$%__@";
    BENCHMARK("Insert long key, long value"){
         for (int i = 0; i < cache_size; i++) {
             cache.insert(key + std::to_string(i), value + std::to_string(i));
         }
    };
}

TEST_CASE("benchmark cache inserting with bloom filter", "[BFECache]") {
    int cache_size = 1000;
    std::string key = "test_key";
    std::string value = "test_value";

    BFECache<std::string, std::string> cache(65536, 3, key.size() + 4);
    std::shared_ptr<InMemCache<std::string, std::string> > c;
    c = std::make_shared<LRUCache<std::string, std::string> >();
    cache.set_cache(c);

    cache.set_max_size(cache_size);

    BENCHMARK("Insert Until Full") {
       for (int i = 0; i < cache_size; i++) {
           cache.insert(key + std::to_string(i), value + std::to_string(i));
       }
    };

    BENCHMARK("Insert After Full") {
       for (int i = 0; i < cache_size; i++) {
           cache.insert(key + std::to_string(i), value + std::to_string(i));
       }
    };
}