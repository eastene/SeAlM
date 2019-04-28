//
// Created by evan on 4/19/19.
//

#include <cassert>
#include "in_mem_cache.hpp"


/*
 * IN MEMORY CACHE
 */

cache::DummyCache::DummyCache() {
    _max_cache_size = 900000; // num of elements
    // Metrics
    _hits = 0;
    _misses = 0;
    _keys = 0;
}

void cache::DummyCache::add_batch(const std::vector<Read> &batch, const std::vector<std::string> &alignments) {}

void cache::DummyCache::evict() {}

std::unordered_map<std::string, std::string>::iterator cache::DummyCache::find(const Read &item) {
    return _cache.find(item[1]);
}

std::unordered_map<std::string, std::string>::iterator cache::DummyCache::end() {
    return _cache.end();
}

std::string& cache::DummyCache::at(const Read &item) {
    return _cache.at(item[1]);
}

std::string &cache::DummyCache::operator[](Read &item) {
    return (std::string &) "";
}


/*
 * LRU CACHE
 */

cache::LRUCache::LRUCache() : InMemCache() {
    // Preallocate space for cache
    _order.resize(_max_cache_size);
    _order_lookup.reserve(_max_cache_size);
    _lru_cache.reserve(_max_cache_size);
}

void cache::LRUCache::evict() {
    std::string key = _order.back();
    _order.pop_back();
    _order_lookup.erase(key);
    _lru_cache.erase(key);
}

void cache::LRUCache::add_batch(const std::vector<Read> &batch, const std::vector<std::string> &alignments) {
    assert(batch.size() == alignments.size());
    for (uint32_t i = 0; i < batch.size(); i+=_skip) {
        if (_lru_cache.find(batch[i][1]) == _lru_cache.end()) {
            if (_lru_cache.size() >= _max_cache_size) {
                _skip = 50;
                evict();
            }
            _order.emplace_front(batch[i][1]);
            _order_lookup.emplace(batch[i][1], _order.begin());
            _lru_cache.emplace(batch[i][1], alignments[i]);
        }
        // only change recency of read on access, not on addition
    }
    _keys = _lru_cache.size();
}

std::unordered_map<std::string, std::string>::iterator cache::LRUCache::find(const Read &item) {
    _lru_cache.find(item[1]) != _lru_cache.end() ? _hits++ : _misses++;
    return _lru_cache.find(item[1]);
}

std::unordered_map<std::string, std::string>::iterator cache::LRUCache::end() {
    return _lru_cache.end();
}

std::string& cache::LRUCache::at(const Read &item) {
    _order.erase(_order_lookup[item[1]]);
    _order.emplace_front(item[1]);
    _order_lookup.insert_or_assign(item[1], _order.begin());
    return _lru_cache.at(item[1]);
}

std::string &cache::LRUCache::operator[](Read &item) {
    return _lru_cache[item[1]];
}


/*
 * MRU CACHE
 */

void cache::MRUCache::evict() {
    std::string key = _order.front();
    _order.pop_front();
    _order_lookup.erase(key);
    _lru_cache.erase(key);
}