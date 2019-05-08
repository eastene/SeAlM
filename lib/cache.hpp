//
// Created by evan on 4/19/19.
//

#ifndef ALIGNER_CACHE_CACHE_HPP
#define ALIGNER_CACHE_CACHE_HPP

#include <iostream>
#include <string>
#include <list>
#include <vector>
#include <unordered_map>
#include "../src/types.hpp"

template<typename K, typename V>
class InMemCache {
protected:
    // Storage structure
    std::unordered_map<K, V> _cache;

    // Size limits
    uint64_t _max_cache_size; // num of elements

    // Metrics
    uint64_t _hits;
    uint64_t _misses;
    uint64_t _keys;

    // Protected methods
    virtual void evict() = 0;

public:

    /*
     *
     * Implemented Base Methods
     *
     */

    InMemCache() : _max_cache_size{900000}, _hits{0}, _misses{0}, _keys{0} {};

    double hit_rate() {
        return _misses > 0 ? _hits / (_misses + _hits) : 0;
    }

    uint64_t hits() { return _hits; }

    uint32_t capacity() { return _max_cache_size; }

    uint32_t size() { return _cache.size(); }

    virtual typename std::unordered_map<K, V>::iterator end() { return _cache.end(); }

    /*
     *
     *  Abstract Methods
     *
     */

    virtual void insert(const K &key, const V &value) = 0;

    virtual void insert_no_evict(const K &key, const V &value) = 0;

    virtual void trim() = 0;

    virtual typename std::unordered_map<K, V>::iterator find(const K &key) = 0;

    virtual V &at(const K &key) = 0;

    virtual V &operator[](K &key) = 0;

    friend std::ostream &operator<<(std::ostream &output, const InMemCache &C) {
        output << "Hits: " << C._hits << " Misses: " << C._misses << " Size: " << C._keys;
        return output;
    }
};

template<typename K, typename V>
class DummyCache : public InMemCache<K, V> {
private:
    void evict() override;

public:

    DummyCache() = default;

    void insert(const K &key, const V &value) override;

    void insert_no_evict(const K &key, const V &value) override;

    void trim();

    typename std::unordered_map<K, V>::iterator find(const K &key) override;

    V &at(const K &key) override;

    V &operator[](K &key) override;
};

template<typename K, typename V>
class LRUCache : public InMemCache<K, V> {
protected:
    std::list<K> _order;
    std::unordered_map<K, typename std::list<K>::iterator> _order_lookup;

    void evict() override;

public:

    LRUCache();

    void insert(const K &key, const V &value) override;

    void insert_no_evict(const K &key, const V &value) override;

    void trim();

    typename std::unordered_map<K, V>::iterator find(const K &key) override;

    V &at(const K &key) override;

    V &operator[](K &key) override;
};


template<typename K, typename V>
class MRUCache : public LRUCache<K, V> {
    void evict() override;
};


/*
 *  Dummy Cache
 */

template<typename K, typename V>
void DummyCache<K, V>::evict() {}

template<typename K, typename V>
void DummyCache<K, V>::insert(const K &key, const V &value) {}

template<typename K, typename V>
void DummyCache<K, V>::insert_no_evict(const K &key, const V &value) {}

template<typename K, typename V>
void DummyCache<K, V>::trim() {}

template<typename K, typename V>
typename std::unordered_map<K, V>::iterator DummyCache<K, V>::find(const K &key) {
    return this->_cache.find(key);
}

template<typename K, typename V>
V &DummyCache<K, V>::at(const K &key) {
    return this->_cache.at(key);
}

template<typename K, typename V>
V &DummyCache<K, V>::operator[](K &key) {
    return key;
}


/*
 *  LRU CACHE
 */

template<typename K, typename V>
LRUCache<K, V>::LRUCache() : InMemCache<K, V>() {
    // Preallocate space for cache
    //_order.resize(this->_max_cache_size);
    _order_lookup.reserve(this->_max_cache_size);
    this->_cache.reserve(this->_max_cache_size);
}

template<typename K, typename V>
void LRUCache<K, V>::evict() {
    std::string key = _order.back();
    _order.pop_back();
    _order_lookup.erase(key);
    this->_cache.erase(key);
    this->_keys--;
}

template<typename K, typename V>
void LRUCache<K, V>::insert(const K &key, const V &value) {
    if (this->_cache.find(key) == this->_cache.end()) {
        if (this->_cache.size() >= this->_max_cache_size) {
            evict();
        }
        _order.emplace_front(key);
        _order_lookup.emplace(key, _order.begin());
        this->_cache.emplace(key, value);

        // only change recency of read on access, not on addition
        this->_keys++;
    }

}

template<typename K, typename V>
void LRUCache<K, V>::insert_no_evict(const K &key, const V &value) {
    if (this->_cache.find(key) == this->_cache.end()) {
        _order.emplace_front(key);
        _order_lookup.emplace(key, _order.begin());
        this->_cache.emplace(key, value);

        // only change recency of read on access, not on addition
        this->_keys++;
    }

}

template<typename K, typename V>
void LRUCache<K, V>::trim() {
    for (uint64_t i = this->_cache.size(); i > this->_max_cache_size; i--) {
        evict();
    }
}

template<typename K, typename V>
typename std::unordered_map<K, V>::iterator LRUCache<K, V>::find(const K &key) {
    this->_cache.find(key) != this->_cache.end() ? this->_hits++ : this->_misses++;
    return this->_cache.find(key);
}

template<typename K, typename V>
V &LRUCache<K, V>::at(const K &key) {
    _order.erase(_order_lookup[key]);
    _order.emplace_front(key);
    _order_lookup.insert_or_assign(key, _order.begin());
    return this->_cache.at(key);
}

template<typename K, typename V>
V &LRUCache<K, V>::operator[](K &key) {
    return this->_cache[key];
}


/*
 * MRU CACHE
 */
template<typename K, typename V>
void MRUCache<K, V>::evict() {
    std::string key = this->_order.front();
    this->_order.pop_front();
    this->_order_lookup.erase(key);
    this->_cache.erase(key);
}

#endif //ALIGNER_CACHE_CACHE_HPP
