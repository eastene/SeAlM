//
// Created by evan on 4/22/19.
//

#ifndef ALIGNER_CACHE_PIPELINE_HPP
#define ALIGNER_CACHE_PIPELINE_HPP

#include <variant>
#include <memory>
#include "../src/types.hpp"
#include "cache.hpp"
#include "io.hpp"

enum CompressionLevel {
    NONE,
    CROSS,
    FULL
};


/*
 *
 *  BUCKETED PIPELINE MANAGER
 *
 *  Orchestrates all bucket io, caching, and pre/post-processing
 *
 */

// T-dataType, R-Reducedtype V-cacheValue, C-Cache
template<typename T, typename R, typename V, typename C>
class BucketedPipelineManager {
protected:
    // IO variables
    InterleavedIOScheduler<T> io;

    // Global cache variables
    C _cache;

    // Current bucket variables
    CompressionLevel _compression_level;
    std::unordered_map<std::string, uint32_t> _duplicate_finder;
    uint64_t _reduced_len;
    uint64_t _total_len;
    uint64_t _max_bucket_size;
    std::vector<T> _unique_bucket;
    std::vector<std::pair<uint64_t, R> > _reduced_bucket;

    // State variables
    bool _ready_for_next; // previous bucket is ready to write

    // Functors

    std::function<V(T)> postprocess_fn;

    // Private methods
    std::unique_ptr<std::vector<T>> reduce_bucket(std::unique_ptr<std::vector<T>> bucket);

public:
    /*
     * Constructors
     */

    BucketedPipelineManager();

    /*
     * Managed Operations
     */

    void open();

    std::unique_ptr<std::vector<T>> read();

    void write(const std::vector<V> &alignment);

    void close();

    /*
     * State descriptors
     */

    bool empty() { return io.empty(); }

    uint64_t current_bucket_size() { return _reduced_bucket.size(); }

    uint64_t cache_hits() { return _cache->hits(); }

    /*
     * Operator Overloads
     */

    friend std::ostream &operator<<(std::ostream &output, const BucketedPipelineManager &B) {
        output << *B._cache << std::endl;
        return output;
    }
};

template<typename T, typename R, typename V, typename C>
BucketedPipelineManager<T, R, V, C>::BucketedPipelineManager() {
    _reduced_len = 0;
    _total_len = 0;
    _max_bucket_size = 50000;
    _compression_level = NONE;
    _ready_for_next = false;
    //TODO: initialize io and cache
}

template<typename T, typename R, typename V, typename C>
std::unique_ptr<std::vector<T>>
BucketedPipelineManager<T, R, V, C>::reduce_bucket(std::unique_ptr<std::vector<T>> bucket) {
    for (const Read &read : bucket) {
//        if (_cache->find(read[1]) != _cache->end()) {
//            cached_alignment = _cache->at(read[1]);
//            _reduced_batch[i] = std::make_tuple(cached_alignment, read[0], read[3]);
//        } else {
//            _reduced_batch[i] = std::make_tuple(_reduced_len++, read[0], read[3]);
//            _unique_batch.emplace_back(read);
//
//        }
//        i++;
    }
}

template<typename T, typename R, typename V, typename C>
void BucketedPipelineManager<T, R, V, C>::open() {
    io.begin_reading();
    _ready_for_next = true;
}

template<typename T, typename R, typename V, typename C>
std::unique_ptr<std::vector<T>> BucketedPipelineManager<T, R, V, C>::read() {
    std::unique_ptr<std::vector<std::pair<uint64_t, T> > > next_bucket = io.request_bucket();

    reduce_bucket(next_bucket);

    switch (_compression_level) {
        case NONE:
            return;
        case CROSS:
            return;
        case FULL:
            return;
        default:
            return;
    }
//    _reduced_len = 0;
//    _total_len = 0;
//    _unique_batch.clear();
//    //_batch.clear();
//
//    std::string cached_alignment;
//    uint32_t i = 0;
//    for (const Read &read : batch) {
//        if (_cache->find(read[1]) != _cache->end()) {
//            cached_alignment = _cache->at(read[1]);
//            _reduced_batch[i] = std::make_tuple(cached_alignment, read[0], read[3]);
//        } else {
//            _reduced_batch[i] = std::make_tuple(_reduced_len++, read[0], read[3]);
//            _unique_batch.emplace_back(read);
//
//        }
//        i++;
//    }
//    if (i < _batch_size)
//        _reduced_batch.resize(i);
//    _total_len = batch.size();
}

template<typename T, typename R, typename V, typename C>
void BucketedPipelineManager<T, R, V, C>::write(const std::vector<V> &out) {
    assert(_unique_bucket.size() == out.size());
    for (uint32_t i = 0; i < _unique_bucket.size(); i++)
        // TODO: keep read sequences for caching as key indexes with lambda for extraction
        _cache->insert(_unique_bucket[i], out[i]);

    //TODO: add lambda for post processing before writing
    for (const auto &item : _reduced_bucket) {
        io.write_async(item.first, postprocess_fn(item.second));
    }
}

template<typename T, typename R, typename V, typename C>
void BucketedPipelineManager<T, R, V, C>::close() {
    io.stop_reading();
    io.flush();
}

// TODO: delete after integrating into pipeline manager (full compression)
//void CompressedBucketManager::dedupe_batch(std::vector<Read> &batch) {
//    _reduced_len = 0;
//    _total_len = 0;
//    _unique_batch.clear();
//    _batch.clear();
//
//    std::string cached_alignment;
//    uint32_t i = 0;
//    std::sort(batch.begin(), batch.end(), [](const Read &a, const Read &b) {
//        return a[1] < b[1];
//    });
//    for (const Read &read : batch) {
//        if (_cache->find(read[1]) != _cache->end()) {
//            cached_alignment = _cache->at(read[1]);
//            _reduced_batch[i] = std::make_tuple(cached_alignment, read[0], read[3]);
//        } else if (_duplicate_finder.find(read[1]) == _duplicate_finder.end()) {
//            _duplicate_finder.emplace(read[1], _reduced_len);
//            _reduced_batch[i] = std::make_tuple(_reduced_len++, read[0], read[3]);
//            _unique_batch.emplace_back(read);
//        } else {
//            _reduced_batch[i] = std::make_tuple(_duplicate_finder[read[1]], read[0], read[3]);
//        }
//        i++;
//    }
//    _total_len = batch.size();
//}

#endif //ALIGNER_CACHE_PIPELINE_HPP
