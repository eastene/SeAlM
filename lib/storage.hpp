//
// Created by evan on 4/30/19.
//

#ifndef ALIGNER_CACHE_STORAGE_HPP
#define ALIGNER_CACHE_STORAGE_HPP

#include <list>
#include <mutex>
#include <atomic>
#include <future>
#include <memory>
#include <vector>
#include <iostream>
#include <algorithm>

template<typename T>
class BufferedBuckets {
private:
    // sizing limits
    uint16_t _max_buckets;
    uint32_t _max_bucket_size;

    // state descriptors
    uint64_t _num_buckets;
    uint64_t _size;
    std::vector<uint16_t> _chain_lengths;
    uint16_t _current_chain;
    typename std::list<std::unique_ptr<std::vector<T> > >::iterator _next_bucket_itr;

    // locks for thread safety
    std::mutex _bucket_mutex;

    // data structures
    std::vector<std::list<std::unique_ptr<std::vector<T> > > > _buckets; // full buckets
    std::vector<std::unique_ptr<std::vector<T> > > _buffers; // partially filled buckets

    // private methods
    void initialize();

    uint16_t hash_func(const T &data);

    void add_bucket(uint16_t from_buffer, std::atomic_bool &cancel);

public:

    BufferedBuckets() {
        _max_buckets = 4;
        _max_bucket_size = 50000;
        initialize();
    }

    void insert(const T &data, std::atomic_bool &cancel);

    void insert_async(const T &data, const std::chrono::milliseconds &timeout);

    std::unique_ptr<std::vector<T>> next_bucket(std::atomic_bool &cancel);

    std::unique_ptr<std::vector<T>> next_bucket_async(const std::chrono::milliseconds &timeout);

    uint64_t size() { return _size; }

    uint64_t num_buckets() { return _num_buckets; }

    bool full() { return _num_buckets >= _max_buckets; }
};


/*
 *  Method Implementations
 */

template<typename T>
void BufferedBuckets<T>::initialize() {
    _size = 0;
    _num_buckets = 0;
    _buckets.resize(_max_buckets);
    _buffers.resize(_max_buckets);
    _chain_lengths.resize(_max_buckets);
    for (uint16_t i = 0; i < _max_buckets; i++) {
        _buffers[i] = std::make_unique<std::vector<T>>();
        _chain_lengths[i] = 0;
    }
    _current_chain = 0;
    _next_bucket_itr = _buckets[0].end();
}

template<typename T>
uint16_t BufferedBuckets<T>::hash_func(const T &data) {
    switch (data[1][0]) {
        case 'A':
            return 0;
        case 'C':
            return 1;
        case 'T':
            return 2;
        case 'G':
            return 3;
        default:
            return 3;
    }
}

template<typename T>
void BufferedBuckets<T>::add_bucket(uint16_t from_buffer, std::atomic_bool &cancel) {
    while (full() && !cancel);
    if (!cancel){
        // acquire lock on bucket structure
        std::lock_guard<std::mutex> lock(_bucket_mutex);
        // add buffer data to new bucket and reset buffer
        _buckets[from_buffer].emplace_back(std::move(_buffers[from_buffer]));
        _buffers[from_buffer] = std::make_unique<std::vector<T>>();
        // if this is the first bucket in empty structure, point next bucket to it
        if (_next_bucket_itr == _buckets[0].end()) {
            _current_chain = from_buffer;
            _next_bucket_itr = _buckets[_current_chain].begin();
        }
        // adjust state after bucket production
        _chain_lengths[from_buffer]++;
        _num_buckets++;
    } else {
        //TODO: throw exception (?), perform any cleanup
    }
}

template<typename T>
void BufferedBuckets<T>::insert(const T &data, std::atomic_bool &cancel) {
    // find buffer for data and add
    uint64_t i = hash_func(data);
    _buffers[i]->emplace_back(data);
    _size++;
    // bucketize buffer if full and buffer space available
    if (_buffers[i]->size() >= _max_bucket_size) {
        add_bucket(i, cancel);
    }
}

template<typename T>
void BufferedBuckets<T>::insert_async(const T &data, const std::chrono::milliseconds &timeout) {
    std::atomic_bool cancel = false;
    std::future<std::unique_ptr<std::vector<T> > > future = std::async(std::launch::async,
                                                                          [&]() { return insert(data, cancel); });
    std::future_status status;
    status = future.wait_for(timeout);
    cancel = true;

    if (status != std::future_status::ready) {
        //TODO: throw exception (?)
    }
}

template<typename T>
std::unique_ptr<std::vector<T>> BufferedBuckets<T>::next_bucket(std::atomic_bool &cancel) {
    while (_num_buckets <= 0 && !cancel);
    if (!cancel) {
        // acquire lock on bucket structure
        std::lock_guard<std::mutex> lock(_bucket_mutex);
        // retrieve next bucket and remove from chain
        std::unique_ptr<std::vector<T>> out = std::move(_buckets[_current_chain].front());
        _buckets[_current_chain].pop_front();
        // consume chains until empty, then move to next chain, chosen as longest chain
        _chain_lengths[_current_chain]--;
        if (_buckets[_current_chain].empty()) {
            uint16_t max_elem = 0;
            for (uint32_t i = 0; i < _chain_lengths.size(); i++) {
                if (_chain_lengths[i] > max_elem) {
                    max_elem = _chain_lengths[i];
                    _current_chain = i;
                }
            }
        }
        // point to next bucket for consumption
        _next_bucket_itr = _buckets[_current_chain].begin();
        // adjust state after consumption
        _size -= out->size();
        _num_buckets--;
        return out;
    } else {
        return nullptr;
    }
}

template<typename T>
std::unique_ptr<std::vector<T>> BufferedBuckets<T>::next_bucket_async(const std::chrono::milliseconds &timeout) {
    std::atomic_bool cancel = false;
    std::future<std::unique_ptr<std::vector<T> > > future = std::async(std::launch::async,
                                                                          [&]() { return next_bucket(cancel); });
    std::future_status status;
    status = future.wait_for(timeout);
    cancel = true;

    if (status == std::future_status::timeout) {
        // separate out this status for future event handling of timed out future
        return nullptr;
    } else if (status == std::future_status::ready) {
        return future.get();
    } else {
        // deferred, also separated out for future event handling of deferred future
        return nullptr;
    }
}


#endif //ALIGNER_CACHE_STORAGE_HPP
