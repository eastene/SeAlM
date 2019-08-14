//
// Created by evan on 4/19/19.
//

#ifndef ALIGNER_CACHE_CACHE_HPP
#define ALIGNER_CACHE_CACHE_HPP

#include <iostream>
#include <string>
#include <list>
#include <vector>
#include <mutex>
#include <unordered_map>
#include <random>
#include "../src/types.hpp"
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
     * Implemented Base Methods
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
};

template<typename K, typename V>
LRUCache<K, V>::LRUCache() : InMemCache<K, V>() {
    // Preallocate space for cache
    //_order.resize(this->_max_cache_size);
    _order_lookup.reserve(this->_max_cache_size + 60000);
    this->_cache_index.reserve(this->_max_cache_size + 60000);
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
    for (uint64_t i = this->_cache_index.size(); i >= this->_max_cache_size; i--) {
        evict();
    }
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


///*
// * CHAIN DECAY CACHE
// */
//
//
//template<typename K, typename V>
//class ChainDecayCache : public LRUCache<K, V> {
//protected:
//    // Decay parameters
//    float lambda;
//    std::default_random_engine generator;
//    std::bernoulli_distribution keep_prob;
//
//public:
//
//    ChainDecayCache() : LRUCache<K, V>() {
//        lambda = 0.8;
//        std::bernoulli_distribution new_prob(0.99);
//        keep_prob.param(new_prob.param());
//    };
//
//    void insert(const K &key, const V &value) override;
//
//    void insert_no_evict(const K &key, const V &value) override;
//
//    void update(int event) {
//        if (event == 0) {
//            // EOC Event
//            std::bernoulli_distribution new_prob(0.99);
//            keep_prob.param(new_prob.param());
//        } else if (event == 1) {
//            // EOB event
//            std::bernoulli_distribution new_prob(keep_prob.p() * lambda);
//            keep_prob.param(new_prob.param());
//        }
//    };
//
//};
//
//template<typename K, typename V>
//void ChainDecayCache<K, V>::insert(const K &key, const V &value) {
//    if (keep_prob(generator)) {
//        std::lock_guard<std::mutex> lock(this->_cache_mutex);
//        if (this->_cache_index.find(key) == this->_cache_index.end()) {
//            if (this->_cache_index.size() >= this->_max_cache_size) {
//                this->evict();
//            }
//            this->_order.emplace_front(key);
//            this->_order_lookup.emplace(key, this->_order.begin());
//            this->_cache_index.emplace(key, value);
//
//            // only change recency of read on access, not on addition
//            this->_keys++;
//        }
//    }
//}
//
//template<typename K, typename V>
//void ChainDecayCache<K, V>::insert_no_evict(const K &key, const V &value) {
//    if (keep_prob(generator)) {
//        std::lock_guard<std::mutex> lock(this->_cache_mutex);
//        if (this->_cache_index.find(key) == this->_cache_index.end()) {
//            this->_order.emplace_front(key);
//            this->_order_lookup.emplace(key, this->_order.begin());
//            this->_cache_index.emplace(key, value);
//
//            // only change recency of read on access, not on addition
//            this->_keys++;
//        }
//    }
//}


#endif //ALIGNER_CACHE_CACHE_HPP
