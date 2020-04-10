//
// Created by evan on 4/19/19.
//

#ifndef SEALM_CACHE_HPP
#define SEALM_CACHE_HPP

#include <list>
#include <mutex>
#include <string>
#include <vector>
#include <random>
#include <iostream>
#include <unordered_map>

#include "types.hpp"
#include "signaling.hpp"

template<typename K, typename V>
class InMemCache : public Observer {
protected:

    // Storage structure
    // TODO: check if shared or unique pointers work best
    std::unordered_map<K, std::unique_ptr<V>> _cache_index;

    // Size limits
    uint64_t _max_cache_size; // num of elements

    // Metrics
    uint64_t _hits;
    uint64_t _misses;
    uint64_t _keys;

    // Locks for thread safety
    std::mutex _cache_mutex;

    // Protected methods
    virtual void evict() = 0;

public:

    /*
     * Implemented Base Methods 1048576 * 4
     */

    InMemCache() : _max_cache_size{1048576 * 4}, _hits{0}, _misses{0}, _keys{0} {};

    void set_max_size(uint64_t max_size) { _max_cache_size = max_size; }

    /*
     * State Descriptors
     */

    double hit_rate() {
        return _misses > 0 ? _hits / (_misses + _hits) : 0;
    }

    uint64_t hits() { return _hits; }

    uint64_t misses() { return _misses; }

    uint32_t capacity() { return _max_cache_size; }

    uint32_t size() { return _cache_index.size(); }

    typename std::unordered_map<K, std::unique_ptr<V>>::iterator end() { return _cache_index.end(); }

    virtual void update(int event) {};

    /*
     *
     *  Abstract Methods
     *
     */

    virtual void insert(const K &key, const V &value) = 0;

    virtual void insert_no_evict(const K &key, const V &value) = 0;

    virtual void trim() = 0;

    virtual typename std::unordered_map<K, std::unique_ptr<V>>::iterator find(const K &key) = 0;

    virtual V &at(const K &key) = 0;

    virtual V &operator[](K &key) = 0;

    virtual void clear() = 0;

    // TODO: find more consistent way to do this?
    virtual void fetch_into(const K &key, V *buff) = 0;

    friend std::ostream &operator<<(std::ostream &output, const InMemCache &C) {
        output << "Hits: " << C._hits << " Misses: " << C._misses << " Size: " << C._keys;
        return output;
    }
};


/*
 *  Dummy Cache
 */


template<typename K, typename V>
class DummyCache : public InMemCache<K, V> {
private:
    void evict() override;

public:

    DummyCache() = default;

    void insert(const K &key, const V &value) override;

    void insert_no_evict(const K &key, const V &value) override;

    void trim();

    typename std::unordered_map<K, std::unique_ptr<V>>::iterator find(const K &key) override;

    V &at(const K &key) override;

    V &operator[](K &key) override;

    void clear() {};

    void fetch_into(const K &key, V *buff) override;
};

template<typename K, typename V>
void DummyCache<K, V>::evict() {}

template<typename K, typename V>
void DummyCache<K, V>::insert(const K &key, const V &value) {}

template<typename K, typename V>
void DummyCache<K, V>::insert_no_evict(const K &key, const V &value) {}

template<typename K, typename V>
void DummyCache<K, V>::trim() {}

template<typename K, typename V>
typename std::unordered_map<K, std::unique_ptr<V>>::iterator DummyCache<K, V>::find(const K &key) {
    return this->_cache_index.find(key);
}

template<typename K, typename V>
V &DummyCache<K, V>::at(const K &key) {
    return *this->_cache_index.at(key);
}

template<typename K, typename V>
V &DummyCache<K, V>::operator[](K &key) {
    return key;
}

template<typename K, typename V>
void DummyCache<K, V>::fetch_into(const K &key, V *buff) {
    buff = nullptr;
}


/*
 *  LRU CACHE
 */


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

    typename std::unordered_map<K, std::unique_ptr<V>>::iterator find(const K &key) override;

    V &at(const K &key) override;

    V &operator[](K &key) override;

    void clear() override;

    void fetch_into(const K &key, V *buff) override;
};

template<typename K, typename V>
LRUCache<K, V>::LRUCache() : InMemCache<K, V>() {
    // Preallocate space for cache
    //_order.resize(this->_max_cache_size);
    _order_lookup.reserve(this->_max_cache_size + 50000);
    this->_cache_index.reserve(this->_max_cache_size + 50000);
}

template<typename K, typename V>
void LRUCache<K, V>::evict() {
    // not publically facing, doesn't require lock (may change in future)
    std::string key = _order.back();
    _order.pop_back();
    _order_lookup.erase(key);
    this->_cache_index.erase(key);
    this->_keys--;
}

template<typename K, typename V>
void LRUCache<K, V>::insert(const K &key, const V &value) {
    std::lock_guard<std::mutex> lock(this->_cache_mutex);
    if (this->_cache_index.find(key) == this->_cache_index.end()) {
        if (this->_cache_index.size() >= this->_max_cache_size) {
            evict();
        }
        _order.emplace_front(key);
        _order_lookup.emplace(key, _order.begin());
        this->_cache_index.emplace(key, std::make_unique<V>(value));

        // only change recency of read on access, not on addition
        this->_keys++;
    }
}

template<typename K, typename V>
void LRUCache<K, V>::insert_no_evict(const K &key, const V &value) {
    // No lock required, adds data only
    //std::lock_guard<std::mutex> lock(this->_cache_mutex);
    if (this->_cache_index.find(key) == this->_cache_index.end()) {
        _order.emplace_front(key);
        _order_lookup.emplace(key, _order.begin());
        this->_cache_index.emplace(key, std::make_unique<V>(value));

        // only change recency of read on access, not on addition
        this->_keys++;
    }
}

template<typename K, typename V>
void LRUCache<K, V>::trim() {
    std::lock_guard<std::mutex> lock(this->_cache_mutex);
    K key;
    for (uint64_t i = this->_cache_index.size(); i >= this->_max_cache_size; i--) {
        key = _order.back();
        _order.pop_back();
        _order_lookup.erase(key);
        this->_cache_index.erase(key);
        this->_keys--;
    }

//    for (uint64_t i = this->_cache_index.size(); i >= this->_max_cache_size; i--) {
//        evict();
//    }
}

template<typename K, typename V>
typename std::unordered_map<K, std::unique_ptr<V>>::iterator LRUCache<K, V>::find(const K &key) {
    std::lock_guard<std::mutex> lock(this->_cache_mutex);
    auto find_ptr = this->_cache_index.find(key);
    find_ptr != this->_cache_index.end() ? this->_hits++ : this->_misses++;
    return find_ptr;
}

template<typename K, typename V>
V &LRUCache<K, V>::at(const K &key) {
    std::lock_guard<std::mutex> lock(this->_cache_mutex);
    _order.erase(_order_lookup[key]);
    _order.emplace_front(key);
    _order_lookup.insert_or_assign(key, _order.begin());
    return *this->_cache_index.at(key);
}

template<typename K, typename V>
V &LRUCache<K, V>::operator[](K &key) {
    std::lock_guard<std::mutex> lock(this->_cache_mutex);
    return *this->_cache_index[key];
}

template<typename K, typename V>
void LRUCache<K, V>::clear() {
    _order.clear();
    _order_lookup.clear();
    this->_cache_index.clear();
}

template<typename K, typename V>
void LRUCache<K, V>::fetch_into(const K &key, V *buff) {
    std::lock_guard<std::mutex> lock(this->_cache_mutex);
    auto find_ptr = this->_cache_index.find(key);
    find_ptr != this->_cache_index.end() ? this->_hits++ : this->_misses++;
    if (find_ptr != this->end()) {
        _order.erase(_order_lookup[key]);
        _order.emplace_front(key);
        _order_lookup.insert_or_assign(key, _order.begin());
        buff = new std::string(*(this->_cache_index.at(key))); //*this->_cache_index.at(key);
    } else {
        // TODO: find a safer return value
        buff = nullptr;
    }
}


/*
 * MRU CACHE
 */


template<typename K, typename V>
class MRUCache : public LRUCache<K, V> {
private:
    void evict() override;

public:
    MRUCache() : LRUCache<K, V>() {};

    void insert_no_evict(const K &key, const V &value) override;

    void trim();
};

template<typename K, typename V>
void MRUCache<K, V>::evict() {
    std::string key = this->_order.front();
    this->_order.pop_front();
    this->_order_lookup.erase(key);
    this->_cache_index.erase(key);
}

template<typename K, typename V>
void MRUCache<K, V>::insert_no_evict(const K &key, const V &value) {
    std::lock_guard<std::mutex> lock(this->_cache_mutex);
    if (this->_cache_index.find(key) == this->_cache_index.end()) {
        this->_order.emplace_front(key);
        this->_order_lookup.emplace(key, this->_order.begin());
        this->_cache_index.emplace(key, std::make_unique<V>(value));

        // only change recency of read on access, not on addition
        this->_keys++;
    }

}

template<typename K, typename V>
void MRUCache<K, V>::trim() {
    std::lock_guard<std::mutex> lock(this->_cache_mutex);
    for (uint64_t i = this->_cache_index.size(); i >= this->_max_cache_size; i--) {
        this->evict();
    }
}


/*
 *
 *
 * CACHE DECORATORS
 *
 *
 */


/*
 * BASE CACHE DECORATOR
 */

template<typename K, typename V>
class CacheDecorator : public InMemCache<K, V> {
protected:
    std::shared_ptr<InMemCache<K, V> > _decorated_cache;

    // implemented as no-op but can be overwritten if decorator implements new evict policy
    // each decorated cache should idealy handle its own eviction policy
    virtual void evict() {  }

public:
    CacheDecorator() {
        // TODO: find a better way to do this (higher level interface?)
        // cache index won't be used, shrink its size to avoid clogging memory
        this->_cache_index.reserve(0);
    }

    void set_cache(std::shared_ptr< InMemCache<K, V> > &cache) { _decorated_cache = cache; }

    /*
     * Overwrite State Descriptors
     */
    void set_max_size(uint64_t max_size) { this->_decorated_cache->set_max_size(max_size); }

    double hit_rate() {
        return this->_decorated_cache->hit_rate();
    }

    uint64_t hits() { return this->_decorated_cache->hits(); }

    uint64_t misses() { return this->_decorated_cache->misses(); }

    uint32_t capacity() { return this->_decorated_cache->capacity(); }

    uint32_t size() { return this->_decorated_cache->size(); }

    typename std::unordered_map<K, std::unique_ptr<V>>::iterator end() { return this->_decorated_cache->end(); }

    friend std::ostream &operator<<(std::ostream &output, const CacheDecorator &C) {
        output << C._decorated_cache;
        return output;
    }
};

/*
* BLOOM FILTER ENHANCED CACHE
*/

template<typename K, typename V>
class BFECache : public CacheDecorator<K, V> {
private:
    uint32_t _m; // number of bits (power of 2)
    uint16_t _k; // number of hash functions (power of 2)
    uint8_t _hash_size; // number of elements considered in each hash
    uint16_t _data_len; // number of elements available to hash over
    uint16_t _alphabits; // number of bits required to represent alphabet TODO: make this a parameter

    std::vector<std::vector<u_int16_t> > _funcs;
    std::vector<bool> _bits;

    void initialize_bloom_filter();

public:
    uint64_t hash_key(const K &key, uint16_t func);

    void add_key(const K &key); // add key to bloom filter
    bool possibly_exists(const K &key); // check if key exists (possibly returns true if key doesn't exist)
    std::vector<std::vector<u_int16_t> > get_functions() { return _funcs; }

public:
    BFECache();

    BFECache(uint32_t m, uint16_t k, uint16_t data_len);

    void set_bloom_params(uint32_t m, uint16_t k, uint16_t data_len){_m=m; _k=k; _data_len=data_len; initialize_bloom_filter();}

    void insert(const K &key, const V &value) override;

    void insert_no_evict(const K &key, const V &value) override;

    void trim();

    typename std::unordered_map<K, std::unique_ptr<V>>::iterator find(const K &key) override;

    V &at(const K &key) override;

    V &operator[](K &key) override;

    void clear() override;

    void fetch_into(const K &key, V *buff) override;

};

template<typename K, typename V>
void BFECache<K, V>::initialize_bloom_filter() {
    _alphabits = 3;
    _bits.resize(_m, 0);
    // random number generator to generate functions
    std::default_random_engine generator;
    generator.seed(1234);
    // TODO: use a distribution that takes into account error profile and known prefix (e.g. Gamma)
    std::uniform_int_distribution<u_int16_t> distribution(0, _data_len - 1);

    _hash_size = ceil(log2(_m) / _alphabits);
    // create functions
    _funcs.resize(_k);
    for (int i = 0; i < _k; i++) {
        for (int j = 0; j < _hash_size; j++) {
            _funcs[i].emplace_back(distribution(generator));
        }
    }
}

template<typename K, typename V>
uint64_t BFECache<K, V>::hash_key(const K &key, uint16_t func) {
    uint64_t hash_res = 0;
    uint8_t shift_amnt = 0;

    for (int i = 0; i < _hash_size; i++) {
        switch (key[_funcs[func][i]]) {
            case 'A':
                hash_res |= 0 << shift_amnt;
                break;
            case 'C':
                hash_res |= 1 << shift_amnt;
                break;
            case 'G':
                hash_res |= 2 << shift_amnt;
                break;
            case 'T':
                hash_res |= 3 << shift_amnt;
                break;
            case 'N':
                hash_res |= 4 << shift_amnt;
                break;
            default:
                // case where unknown character is found or reads are trimmed (thus too short for function)
                hash_res |= 7 << shift_amnt;
                break;
        }
        shift_amnt += _alphabits;
    }

    return hash_res;
}

template<typename K, typename V>
void BFECache<K, V>::add_key(const K &key) {
    std::lock_guard<std::mutex> lock(this->_cache_mutex);
    for (int i = 0; i < _k; i++) {
        _bits[hash_key(key, i)] = 1;
    }
}

template<typename K, typename V>
bool BFECache<K, V>::possibly_exists(const K &key) {
    std::lock_guard<std::mutex> lock(this->_cache_mutex);
    for (int i = 0; i < _k; i++) {
        if (!_bits[hash_key(key, i)])
            return false;
    }
    return true;
}

template<typename K, typename V>
BFECache<K, V>::BFECache() {
    // choose m and k to get an epsilon of ~5%, will be less due to rounding to nearest pow of 2
    // assumes cache holds default number of elements (4M keys)
    _m = 1 < 24;
    _k = 1 < 2;
    _hash_size = 8;  // assumes 3 bits per base
    _data_len = 100;

    initialize_bloom_filter();
}

template<typename K, typename V>
BFECache<K, V>::BFECache(uint32_t m, uint16_t k, uint16_t data_len) {
    // choose m and k to get an epsilon of ~5%, will be less due to rounding to nearest pow of 2
    // assumes cache holds default number of elements (4M keys)
    _m = m;
    _k = k;
    _hash_size = 2;  // assumes 3 bits per base
    _data_len = data_len;

    initialize_bloom_filter();
}

template<typename K, typename V>
void BFECache<K, V>::insert(const K &key, const V &value) {
    if (possibly_exists(key)) {
        // key already seen, good candidate for cache
        this->_decorated_cache->insert(key, value);
    } else {
        // remember key, but don't add to cache
        add_key(key);
    }
}

template<typename K, typename V>
void BFECache<K, V>::insert_no_evict(const K &key, const V &value) {
    if (possibly_exists(key)) {
        // key already seen, good candidate for cache
        this->_decorated_cache->insert_no_evict(key, value);
    } else {
        // remember key, but don't add to cache
        add_key(key);
    }
}

template<typename K, typename V>
void BFECache<K, V>::trim() {
    this->_decorated_cache->trim();
}

template<typename K, typename V>
typename std::unordered_map<K, std::unique_ptr<V>>::iterator BFECache<K, V>::find(const K &key) {
    if (!possibly_exists(key)) {
        // use bloom filter to prevent unecessary cache searches
        return this->_decorated_cache->end();
    } else {
        add_key(key); // TODO: should key be added to bloom filter here? or only on insert?
        return this->_decorated_cache->find(key);
    }
}

template<typename K, typename V>
V &BFECache<K, V>::at(const K &key) {
    return this->_decorated_cache->at(key);
}

template<typename K, typename V>
V &BFECache<K, V>::operator[](K &key) {
    return this->_decorated_cache->operator[](key);
}

template<typename K, typename V>
void BFECache<K, V>::clear() {
    // reset bloom filter
    _bits.clear();
    _bits.resize(_m, 0);
    this->_decorated_cache->clear();
}

template<typename K, typename V>
void BFECache<K, V>::fetch_into(const K &key, V *buff) {
    this->_decorated_cache->fetch_into(key, buff);
}

#endif //SEALM_CACHE_HPP
