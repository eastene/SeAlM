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
    // Sizing limits
    uint64_t _max_buckets;
    uint64_t _table_width;
    uint64_t _max_bucket_size;

    // State variables
    uint64_t _num_full_buckets;
    uint64_t _size;
    std::vector<uint16_t> _chain_lengths;
    uint64_t _current_chain;
    typename std::list<std::unique_ptr<std::vector<T> > >::iterator _next_bucket_itr;

    // Atomic variables
    bool _alive;

    // Locks for thread safety
    std::mutex _bucket_mutex;

    // Data structures
    std::vector<std::list<std::unique_ptr<std::vector<T> > > > _buckets; // full buckets
    std::vector<std::unique_ptr<std::vector<T> > > _buffers; // partially filled buckets

    // Functors
    std::function<uint64_t(const T &)> _hash_fn;

    // Private methods
    void initialize();

    void add_bucket(uint64_t from_buffer);

public:

    /*
     *  Constructors
     */

    BufferedBuckets();

    BufferedBuckets(uint64_t max_buckets, uint64_t max_bucket_size);

    /*
     * Consumption Methods
     */

    bool insert(const T &data);

    std::future<bool> insert_async(const T &data);

    /*
     * Production Methods
     */

    std::unique_ptr<std::vector<T>> next_bucket();

    std::future<std::unique_ptr<std::vector<T> > > next_bucket_async();

    /*
     * Forcing Methods
     */

    void flush();

    void kill() { _alive = false; } // stop all read/writes pending if in deadlock, puts store in unsafe state

    // TODO: implement deadlock recovery
    bool recover() { if (!_alive) _alive = true; }

    /*
     * State Descriptors
     */

    uint64_t size() { return _size; }

    uint64_t num_buckets() { return _num_full_buckets; }

    bool full() { return _num_full_buckets >= _max_buckets; }

    bool empty() { return _num_full_buckets == 0; }

    /*
     * Getters/Setters
     */

    uint64_t capacity() { return _max_buckets; }

    void set_num_buckets(uint64_t max_buckets) { _max_buckets = max_buckets; }

    void set_bucket_size(uint64_t max_bucket_size) { _max_bucket_size = max_bucket_size; }

    void set_hash_fn(std::function<uint64_t(const T &)> fn) { _hash_fn = fn; }

    // TODO: add resize funtion that doesn't reinitialize all state
    void set_table_width(uint64_t table_width) { _table_width = table_width; initialize(); }

    /*
     * Operator Overloads
     */
    BufferedBuckets &operator=(const BufferedBuckets &other);
};

/*
 * Functors
 */

template<typename T>
uint64_t default_hash(T &data) {
    return 0; // does nothing
}

/*
 *  Method Implementations
 */
template<typename T>
BufferedBuckets<T>::BufferedBuckets() {
    _max_buckets = 1;
    _table_width = 1;
    _max_bucket_size = 100000;
    _hash_fn = std::function<uint64_t(const T &)>([](const T &data) { return default_hash(data); });
    initialize();
}

template<typename T>
BufferedBuckets<T>::BufferedBuckets(uint64_t max_buckets, uint64_t max_bucket_size) {
    _max_buckets = max_buckets;
    _table_width = 1;
    _max_bucket_size = max_bucket_size;

    _hash_fn = std::function<uint64_t(const T &)>([](const T &data) { return default_hash(data); });
    initialize();
}

template<typename T>
void BufferedBuckets<T>::initialize() {
    _size = 0;
    _num_full_buckets = 0;
    _buckets.resize(_table_width);
    _buffers.resize(_table_width);
    _chain_lengths.resize(_table_width);
    for (uint16_t i = 0; i < _table_width; i++) {
        _buffers[i] = std::make_unique<std::vector<T>>();
        _chain_lengths[i] = 0;
    }
    _current_chain = 0;
    _next_bucket_itr = _buckets[0].end();
    _alive = true;
}

template<typename T>
void BufferedBuckets<T>::add_bucket(uint64_t from_buffer) {
    while (full() && _alive);
    if (_alive) {
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
        _num_full_buckets++;
    } else {
        //TODO: throw exception (?), perform any cleanup
    }
}

template<typename T>
bool BufferedBuckets<T>::insert(const T &data) {
    // find buffer for data and add
    //TODO: resize table if i goes beyond bounds
    uint64_t i = _hash_fn(data);
    _buffers[i]->emplace_back(data);
    _size++;
    // bucketize buffer if full and buffer space available
    if (_buffers[i]->size() >= _max_bucket_size) {
        add_bucket(i);
    }

    return true;
}

template<typename T>
std::future<bool> BufferedBuckets<T>::insert_async(const T &data) {
    std::future<bool> future = std::async(std::launch::async, [&]() { return insert(data); });
    return future;
}

template<typename T>
void BufferedBuckets<T>::flush() {
    std::lock_guard<std::mutex> lock(_bucket_mutex);
    for (uint64_t i = 0; i < _buffers.size(); i++) {
        if (!_buffers[i]->empty()) {
            _buckets[i].emplace_back(std::move(_buffers[i]));
            _buffers[i] = std::make_unique<std::vector<T>>();
            _num_full_buckets++;
            _chain_lengths[i]++;
        }
    }
}

template<typename T>
std::unique_ptr<std::vector<T>> BufferedBuckets<T>::next_bucket() {
    while (_num_full_buckets <= 0 && _alive);
    if (_alive) {
        // acquire lock on bucket structure
        std::lock_guard<std::mutex> lock(_bucket_mutex);
        // retrieve next bucket and remove from chain
        std::unique_ptr<std::vector<T>> out = std::move(_buckets[_current_chain].front());
        _buckets[_current_chain].pop_front();
        // consume chains until empty, then move to next chain, chosen as longest chain
        _chain_lengths[_current_chain]--;
        if (_buckets[_current_chain].empty()) {
            std::cout << "Switching chains." << std::endl;
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
        _num_full_buckets--;
        return std::move(out);
    } else {
        return nullptr;
    }
}

template<typename T>
std::future<std::unique_ptr<std::vector<T> > >
BufferedBuckets<T>::next_bucket_async() {
    return std::async(std::launch::async, [&]() { return std::move(next_bucket()); });
}

template<typename T>
BufferedBuckets<T> &BufferedBuckets<T>::operator=(const BufferedBuckets<T> &other) {
    // Sizing limits
    _max_buckets = other._max_buckets;
    _table_width = other._table_width;
    _max_bucket_size = other._max_bucket_size;

    // State variables
//    _num_full_buckets = other._num_full_buckets;
//    _size = other._size;
//    _chain_lengths = other._chain_lengths;
//    _current_chain = other._current_chain;
//
//    _next_bucket_itr = other._next_bucket_itr;
//
//    // Atomic variables
//    _alive = other._alive;

    initialize(); // TODO: find a way to move over unique pointer from other bucket to transfer

    // Locks for thread safety
//    if (!other._bucket_mutex.try_lock()) {
//        _bucket_mutex.lock();
//    } else {
//        other._bucket_mutex.unlock();
//        _bucket_mutex.unlock();
//    }


    // Data structures
//    _buckets.resize(other._buckets.size());
//    for (uint64_t i = 0; i < other._buckets.size(); i++) {
//        for (uint64_t j = 0; j < other._buckets[i].size(); j++){
//            _buckets[i].emplace_back(std::move(other._buckets[i][j]));
//        }
//    }
//    _buffers.resize(other._buffers.size());
//    for (uint64_t i = 0; i < other._buffers.size(); i++) {
//        _buffers[i] = std::move(other._buffers[i]);
//    }

    // Functors
    _hash_fn = other._hash_fn;
}


#endif //ALIGNER_CACHE_STORAGE_HPP
