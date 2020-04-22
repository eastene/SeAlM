//
// Created by evan on 4/30/19.
//

#ifndef SEALM_STORAGE_HPP
#define SEALM_STORAGE_HPP

#include <list>
#include <mutex>
#include <atomic>
#include <future>
#include <memory>
#include <vector>
#include <iostream>
#include <algorithm>

#include "logging.hpp"
#include "signaling.hpp"

/*
 * Storage Specific Exceptions
 */

class BadChainPushException : public std::exception {
};

/*
 *  Storage Specific Types & Interfaces
 */

enum ChainSwitch {
    LONGEST,
    RANDOM
};

// T - sequence Type (string, vector, etc.)
template<typename T>
class DataHasher{
public:
    virtual ~DataHasher(){};
    virtual uint64_t _hash_fn(const T &d) = 0;
    virtual uint64_t _required_table_width() = 0;
};

template<typename T>
struct ValueOrdering {
    bool operator()(T l, T r){
        return l < r;
    }
};

/*
 * ORDERED SEQUENCE STORAGE
 *
 * Base class of types that implement storage for
 * sequence data that require an imposed ordering
 *
 */

// T - sequence Type (string, vector, etc.)
template<typename T>
class OrderedSequenceStorage : public Observable {
protected:
    // Sizing limits
    uint64_t _max_buckets;
    uint64_t _table_width;
    uint64_t _max_bucket_size;
    uint64_t _max_chain_len;

    // State variables
    uint64_t _num_full_buckets;
    uint64_t _size;
    std::vector<uint16_t> _chain_lengths;
    uint64_t _current_chain;
    bool _flushed;
    ChainSwitch _chain_switch;

    // Atomic variables
    bool _alive;

    // Locks for thread safety
    std::mutex _bucket_mutex;

    // Template functions class for deriving data-specific hashes
    std::shared_ptr< DataHasher<T> > _hash;

    // Comparator
    ValueOrdering<T> _order;

    // Protected methods
    virtual void initialize() = 0;
    virtual void add_bucket(uint64_t from_buffer) = 0;

public:

    /*
     * Constructors
     */
    OrderedSequenceStorage() {
        _max_chain_len = std::numeric_limits<uint64_t>::max();
        _num_full_buckets = 0;
        _size = 0;
        _current_chain = 0;
        _alive = true;
        _flushed = false;
        _chain_switch = ChainSwitch::LONGEST;

        _max_buckets = 1;
        _table_width = 1;
        _max_bucket_size = 100000;
    }

    virtual ~OrderedSequenceStorage(){};

    /*
     * Consumption Methods
     */

    virtual bool insert(const T &data) = 0;

    std::future<bool> insert_async(const T &data) {
        return std::async(std::launch::async, [&]() {
            return insert(data);
        });
    }

    /*
     * Production Methods
     */

    virtual std::unique_ptr<std::vector<T>> next_bucket() = 0;

    std::future<std::unique_ptr<std::vector<T> > > next_bucket_async() {
        return std::async(std::launch::async, [&]() {
            return std::move(next_bucket());
        });
    }

    /*
     * Forcing Methods
     */

    virtual void flush() = 0;

    void kill() { _alive = false; } // stop all read/writes pending if in deadlock, puts store in unsafe state

    // TODO: implement deadlock recovery
    bool recover() { if (!_alive) _alive = true; return _alive;}

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

    void set_num_buckets(uint64_t max_buckets) {
        if (max_buckets < _table_width){
            log_warn("Allowable buckets less than hash table width, may cause performance issues or lead to deadlock. Suggest increasing max allowable buckets.");
        }
        _max_buckets = max_buckets;
    }

    void set_bucket_size(uint64_t max_bucket_size) { _max_bucket_size = max_bucket_size; }

    void set_chain_switch_mode(ChainSwitch cs) { _chain_switch = cs; }

    // TODO: add resize function that doesn't reinitialize all state
    void set_table_width(uint64_t table_width) {
        if (_max_buckets < table_width){
            log_warn("Allowable buckets less than hash table width, may cause performance issues or lead to deadlock. Suggest increasing max allowable buckets.");
        }
        _table_width = table_width;
        initialize();
    }

    void set_data_properties(std::shared_ptr< DataHasher<T> > &other){
        _hash = other;
        set_table_width(_hash->_required_table_width());
    }

    void set_ordering(ValueOrdering<T> &other){_order = other;}

    /*
     * Operator Overloads
     */
    OrderedSequenceStorage &operator=(const OrderedSequenceStorage &other) {
        // Sizing limits
        _max_buckets = other._max_buckets;
        _table_width = other._table_width;
        _max_bucket_size = other._max_bucket_size;
        _max_chain_len = other._max_chain_len;

        // State variables
        _num_full_buckets = other._num_full_buckets;
        _size = other._size;
        for (int i = 0; i < _chain_lengths.size(); i++) {
            _chain_lengths[i] = other._chain_lengths[i];
        }
        _current_chain = other._current_chain;
        _chain_switch = other._chain_switch;

        // Atomic variables
        _alive = other._alive;

        // Data properties
        _hash = other._hash;
        _order = other._order;
    }
};


/*
 *
 * BUFFERED BUCKETS
 *
 * Bucketed storage for asynchronous I/O of hashed data
 * that is accessed in chains of similar buckets.
 *
 */

template<typename T>
class BufferedBuckets : public OrderedSequenceStorage<T> {
protected:
    typename std::list<std::unique_ptr<std::vector<T> > >::iterator _next_bucket_itr;

    // Data structures
    std::vector<std::list<std::unique_ptr<std::vector<T> > > > _buckets; // full buckets
    std::vector<std::unique_ptr<std::vector<T> > > _buffers; // partially filled buckets

    // Private methods
    void initialize() override;

    void add_bucket(uint64_t from_buffer) override;

public:

    BufferedBuckets() : OrderedSequenceStorage<T>() { initialize(); };

    /*
     * Consumption Methods
     */

    virtual bool insert(const T &data) override;

    /*
     * Production Methods
     */

    virtual std::unique_ptr<std::vector<T>> next_bucket() override;

    /*
     * Forcing Methods
     */

    virtual void flush() override;

    /*
     * Operator Overloads
     */
    BufferedBuckets &operator=(const BufferedBuckets &other);
};


/*
 *  Method Implementations
 */

template<typename T>
void BufferedBuckets<T>::initialize() {
    this->_size = 0;
    this->_num_full_buckets = 0;
    _buckets.resize(this->_table_width);
    _buffers.resize(this->_table_width);
    this->_chain_lengths.resize(this->_table_width);
    for (uint16_t i = 0; i < this->_table_width; i++) {
        _buffers[i] = std::make_unique<std::vector<T>>();
        this->_chain_lengths[i] = 0;
    }
    this->_current_chain = 0;
    _next_bucket_itr = _buckets[0].end();
    this->_alive = true;

    srand(time(NULL));
}

template<typename T>
void BufferedBuckets<T>::add_bucket(uint64_t from_buffer) {
    while (this->full() && this->_alive);

    if (this->_alive) {
        // acquire lock on bucket structure
        std::lock_guard<std::mutex> lock(this->_bucket_mutex);
        // add buffer data to new bucket and reset buffer
        _buckets[from_buffer].emplace_back(std::move(_buffers[from_buffer]));
        _buffers[from_buffer] = std::make_unique<std::vector<T>>();
        // if this is the first bucket in empty structure, point next bucket to it
        if (_next_bucket_itr == _buckets[0].end()) {
            this->_current_chain = from_buffer;
            _next_bucket_itr = _buckets[this->_current_chain].begin();
        }
        // adjust state after bucket production
        this->_chain_lengths[from_buffer]++;
        this->_num_full_buckets++;
    } else {
        //TODO: throw exception (?), perform any cleanup
    }
}

template<typename T>
bool BufferedBuckets<T>::insert(const T &data) {
    // find buffer for data and add
    //TODO: resize table if i goes beyond bounds
    uint64_t i = this->_hash->_hash_fn(data);
    _buffers[i]->emplace_back(data);
    this->_size++;
    // bucketize buffer if full and buffer space available
    if (_buffers[i]->size() >= this->_max_bucket_size) {
        add_bucket(i);
    }

    return true;
}

template<typename T>
void BufferedBuckets<T>::flush() {
    log_debug("Flushing buffers.");
    std::lock_guard<std::mutex> lock(this->_bucket_mutex);
    for (uint64_t i = 0; i < _buffers.size(); i++) {
        if (!_buffers[i]->empty()) {
            _buckets[i].emplace_back(std::move(_buffers[i]));
            _buffers[i] = std::make_unique<std::vector<T>>();
            this->_num_full_buckets++;
            this->_chain_lengths[i]++;
        }
    }
}

template<typename T>
std::unique_ptr<std::vector<T>> BufferedBuckets<T>::next_bucket() {
    while (this->_num_full_buckets <= 0 && this->_alive);
    if (this->_alive) {
        // acquire lock on bucket structure
        std::lock_guard<std::mutex> lock(this->_bucket_mutex);
        // retrieve next bucket and remove from chain
        std::unique_ptr<std::vector<T>> out = std::move(_buckets[this->_current_chain].front());
        _buckets[this->_current_chain].pop_front();
        // consume chains until empty, then move to next chain, chosen as longest chain
        this->_chain_lengths[this->_current_chain]--;

        if (this->_chain_switch == ChainSwitch::RANDOM) {
            this->notify(0);
            // RANDOM CHAIN SWITCHING
            log_debug("Switching chains.");
            uint16_t r = rand() % this->_table_width;
            for (uint32_t i = 0; i < this->_chain_lengths.size(); i++) {
                if (this->_chain_lengths[r] != 0) {
                    this->_current_chain = r;
                    break;
                }
                r = rand() % this->_table_width;
            }
        } else if (_buckets[this->_current_chain].empty()) {
            this->notify(0);
            // STRUCTURED CHAIN SWITCHING
            log_debug("Switching chains.");
            uint16_t max_elem = 0;
            for (uint32_t i = 0; i < this->_chain_lengths.size(); i++) {
                if (this->_chain_lengths[i] > max_elem) {
                    max_elem = this->_chain_lengths[i];
                    this->_current_chain = i;
                }
            }
        }

        // point to next bucket for consumption
        if (_buckets[this->_current_chain].empty()) {
            // empty structure, set next bucket to sentinel value
            _next_bucket_itr = _buckets[0].end();
        } else {
            // some next bucket exists
            _next_bucket_itr = _buckets[this->_current_chain].begin();
        }
        // adjust state after consumption
        this->_size -= out->size();
        this->_num_full_buckets--;
        return std::move(out);
    } else {
        return nullptr;
    }
}

template<typename T>
BufferedBuckets<T> &BufferedBuckets<T>::operator=(const BufferedBuckets<T> &other) {
    // Sizing limits
    this->_max_buckets = other._max_buckets;
    this->_table_width = other._table_width;
    this->_max_bucket_size = other._max_bucket_size;
    this->_chain_switch = other._chain_switch;

    initialize(); // TODO: find a way to move over unique pointer from other bucket to transfer

    // Data property assesors
    this->_hash = other._hash;
    this->_order = other._order;
}

/*
 *
 * BUFFERED SORTED CHAIN
 *
 * Similar to BUFFERED BUCKETS, but has only a single bucket per chain
 * that is in sorted order. Chains are of varying sizes only bounded by some
 * maximum size
 *
 */

template<typename T>
class BufferedSortedChain : public OrderedSequenceStorage<T> {
protected:
    // State variables
    uint64_t _next_chain;

    // Data structures
    std::vector<std::unique_ptr<std::vector<T> > > _sorted_chain; // full, sorted chains
    std::vector<std::unique_ptr<std::vector<T> > > _buffers; // partially filled buckets

    // Protected Methods
    void initialize() override;

    void add_bucket(uint64_t from_buffer) override;

public:
    BufferedSortedChain() : OrderedSequenceStorage<T>() { initialize(); };

    bool insert(const T &data) override;

    std::unique_ptr<std::vector<T>> next_bucket() override;

    void flush() override;

    /*
     * Operator Overloads
     */
    BufferedSortedChain &operator=(const BufferedSortedChain &other);
};

template<typename T>
void BufferedSortedChain<T>::initialize() {
    this->_size = 0;
    this->_num_full_buckets = 0;
    _sorted_chain.resize(this->_table_width);
    this->_buffers.resize(this->_table_width);
    this->_chain_lengths.resize(this->_table_width);
    for (uint16_t i = 0; i < this->_table_width; i++) {
        this->_buffers[i] = std::make_unique<std::vector<T>>();
        this->_chain_lengths[i] = 0;
    }
    this->_current_chain = 0;

    this->_alive = true;
    this->_max_chain_len = 1;
    _next_chain = std::numeric_limits<uint64_t>::max();
}

template<typename T>
void BufferedSortedChain<T>::add_bucket(uint64_t from_buffer) {
    while (this->full() && this->_alive);

    if (this->_chain_lengths[from_buffer] >= this->_max_chain_len) {
        throw BadChainPushException();
    }

    if (this->_alive) {
        // acquire lock on bucket structure
        std::lock_guard<std::mutex> lock(this->_bucket_mutex);
        // add buffer data to new bucket and reset buffer
        _sorted_chain[from_buffer] = std::move(this->_buffers[from_buffer]);
        this->_buffers[from_buffer] = std::make_unique<std::vector<T>>();

        // sort each bucket in place as it is added
        std::sort(_sorted_chain[from_buffer]->begin(), _sorted_chain[from_buffer]->end(), this->_order);

        // if this is the first bucket in empty structure, point next bucket to it
        if (_next_chain == std::numeric_limits<uint64_t>::max()) {
            this->_current_chain = from_buffer;
            _next_chain = from_buffer;
        }
        // adjust state after bucket production
        this->_chain_lengths[from_buffer]++;
        this->_num_full_buckets++;
    } else {
        //TODO: throw exception (?), perform any cleanup
    }
}

template<typename T>
bool BufferedSortedChain<T>::insert(const T &data) {
    // find buffer for data and add
    //TODO: resize table if i goes beyond bounds
    uint64_t i = this->_hash->_hash_fn(data);
    this->_buffers[i]->emplace_back(data);
    this->_size++;
    // bucketize buffer if full and buffer space available
    if (this->_buffers[i]->size() >= this->_max_bucket_size) {
        try {
            add_bucket(i);
        } catch (BadChainPushException &bcpe) {
            flush();
        }
    }

    return true;
}

template<typename T>
std::unique_ptr<std::vector<T>> BufferedSortedChain<T>::next_bucket() {
    while (this->_num_full_buckets <= 0 && this->_alive);
    if (this->_alive) {
        // acquire lock on bucket structure
        std::lock_guard<std::mutex> lock(this->_bucket_mutex);
        // retrieve next bucket and remove from chain
        std::unique_ptr<std::vector<T>> out = std::move(_sorted_chain[this->_current_chain]);
        _sorted_chain[this->_current_chain] = std::make_unique<std::vector<T> >();
        // consume chains until empty, then move to next chain, chosen as longest chain
        this->_chain_lengths[this->_current_chain]--;
        if (_sorted_chain[this->_current_chain]->empty()) {
            uint16_t max_elem = 0;
            for (uint32_t i = 0; i < this->_chain_lengths.size(); i++) {
                if (this->_chain_lengths[i] > max_elem) {
                    max_elem = this->_chain_lengths[i];
                    this->_current_chain = i;
                }
            }
        }
        // point to next bucket for consumption
        if (_sorted_chain[this->_current_chain]->empty()) {
            // empty structure
            _next_chain = std::numeric_limits<uint64_t>::max();
        } else {
            // some next bucket exists
            _next_chain = this->_current_chain;
        }
        // adjust state after consumption
        this->_size -= out->size();
        this->_num_full_buckets--;
        return std::move(out);
    } else {
        return nullptr;
    }
}

template<typename T>
void BufferedSortedChain<T>::flush() {
    std::lock_guard<std::mutex> lock(this->_bucket_mutex);
    for (uint64_t i = 0; i < this->_buffers.size(); i++) {
        if (!this->_buffers[i]->empty()) {
            // TODO add way of handling empty buffers without adding empty buckets
            if (!_sorted_chain[i] || _sorted_chain[i]->empty()) {
                _sorted_chain[i] = std::move(this->_buffers[i]);
                this->_num_full_buckets++;
            } else {
                //this->_buckets[i][0]->reserve(this->_buckets[i][0]->size() + this->_buffers[i]->size());
                _sorted_chain[i]->insert(_sorted_chain[i]->begin() + _sorted_chain[i]->size(),
                                         this->_buffers[i]->begin(),
                                         this->_buffers[i]->end());
                std::sort(_sorted_chain[i]->begin(), _sorted_chain[i]->end(), this->_order);
            }
            this->_buffers[i] = std::make_unique<std::vector<T>>();
            this->_chain_lengths[i] = 1;
        }
    }
}

template<typename T>
BufferedSortedChain<T> &BufferedSortedChain<T>::operator=(const BufferedSortedChain<T> &other) {
    // Sizing limits
    this->_max_buckets = other._max_buckets;
    this->_table_width = other._table_width;
    this->_max_bucket_size = other._max_bucket_size;
    this->_max_chain_len = other._max_chain_len;
    this->_chain_switch = other._chain_switch;

    this->initialize(); // TODO: find a way to move over unique pointer from other bucket to transfer

    // Data properties
    this->_hash = other._hash;
    this->_order = other._order;
}

#endif //SEALM_STORAGE_HPP
