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
    REQUIRE(cache.capacity() == 1048576 * 4);

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
    std::shared_ptr<CacheIndex<std::string, std::string> > c;
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
    int cache_size = 100;
    DummyCache<std::string, std::string> baseline;
    LRUCache<std::string, std::string> cache;
    cache.set_max_size(cache_size);
    std::string key = "test_key";
    std::string value = "test_value";

    BENCHMARK("Insert Baseline (Dummy Cache)") {
           for (int i = 0; i < cache_size; i++) {
               baseline.insert(key + std::to_string(i), value + std::to_string(i));
           }
       };

    BENCHMARK("Insert Until Full") {
        for (int i = 0; i < cache_size; i++) {
            cache.insert_no_evict(key + std::to_string(i), value + std::to_string(i));
        }
    };

    BENCHMARK("Insert After Full") {
       for (int i = 0; i < cache_size; i++) {
           cache.insert_no_evict(key + std::to_string(i), value + std::to_string(i));
       }
       cache.trim();
    };

    BENCHMARK("Baseline Search (Dummy Cache)") {
           for (int i = 0; i < cache_size; i++) {
               if (baseline.find(key + std::to_string(i)) != baseline.end()){
                   value = cache.at(key + std::to_string(i));
               }
           }
       };

    BENCHMARK("Search") {
        for (int i = 0; i < cache_size; i++) {
            if(cache.find(key + std::to_string(i)) != cache.end()){
                value = cache.at(key + std::to_string(i));
            }
        }
    };

    key = "ACGCATCAGCTACAGGAGCTCCTAGAGCGCGAGCGAGCATTACACATTACTCATCTACTAGCAGCGACATCAGCAGCGACAGAGAGAGAGTTTAATTTAC";
    BENCHMARK("Insert Baseline long key, long value (Dummy Cache)") {
           for (int i = 0; i < cache_size; i++) {
               baseline.insert(key + std::to_string(i), value + std::to_string(i));
           }
       };

    value = "ACGCATCAGCTACAGGAGCTCCTAGAGCGCGAGCGAGCATTACACATTACTCATCTACTAGCAGCGACATCAGCAGCGACAGAGAGAGAGTTTAATTTAC ++#+#++$#$+#+#++#+##++#+#+@+#$+++@++!@+#+!+#++$%++^^+&+%^+$+%#+%+@+$@+#$+@+#$+!++++@#$_@#$_@))^$%__@";
    BENCHMARK("Insert long key, long value"){
         for (int i = 0; i < cache_size; i++) {
             cache.insert(key + std::to_string(i), value + std::to_string(i));
         }
    };

    BENCHMARK("Baseline Search long key, long value (Dummy Cache)") {
           for (int i = 0; i < cache_size; i++) {
               if (baseline.find(key + std::to_string(i)) != baseline.end()){
                   value = cache.at(key + std::to_string(i));
               }
           }
       };

    BENCHMARK("Search long key, long value"){
        for (int i = 0; i < cache_size; i++) {
            if(cache.find(key + std::to_string(i)) != cache.end()){
                value = cache.at(key + std::to_string(i));
            }
        }
    };
}

std::string extract_key_fn(std::string &data) {
    // std::vector<bool> bin(ceil(data[1].size() / 3.0), 0);
    char* bin = new char[(data.size() / 2) + data.size() % 2];
    int j = 0;
    int i = 0;
    for (; i < data.size() - 1; i += 2){
        bin[j] = 0b00000000;
        bin[j] |= data[i] & 0b00000111;
        bin[j] <<= 3;
        bin[j] &= 0b00111000;
        bin[j++] |= data[i+1] & 0b00000111;
    }
    // odd-length strings
    if (i++ == data.size() - 1){
        bin[j] = 0b00000000;
        bin[j] |= data[i] & 0b00000111;
    }

    std::string out (bin, j);
    return out;
}

TEST_CASE("benchmark cache compressed inserting with lru eviction" "[LRUCache]") {
    int cache_size = 99;
    LRUCache<std::string, std::string> cache;
    cache.set_max_size(cache_size);
    std::string key = "ACGCATCAGCTACAGGAGCTCCTAGAGCGCGAGCGAGCATTACACATTACTCATCTACTAGCAGCGACATCAGCAGCGACAGAGAGAGAGTTTAATTTACA";
    std::string value = "test_value";

    BENCHMARK("Insert Baseline") {
       cache.insert(key, value);
   };

    cache.insert(key, value);
    BENCHMARK("Baseline Search") {
         if (cache.find(key) != cache.end()){
             value = cache.at(key);
         }
     };

    cache.clear();
    BENCHMARK("Insert Compressed") {
        cache.insert_no_evict(extract_key_fn(key), value);
   };

    cache.insert(extract_key_fn(key), value);
    REQUIRE(cache.find(extract_key_fn(key)) != cache.end());

    BENCHMARK("Search Compressed") {
        if (cache.find(extract_key_fn(key)) != cache.end()){
                value = cache.at(extract_key_fn(key));
        }
    };
}