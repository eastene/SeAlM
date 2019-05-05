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
    NONE,  // no compression of batch, unless in cache
    CROSS, // compression only for duplicates across file ids (not within)
    FULL  // compression of all duplicates in batch
};


/*
 *
 *  BUCKETED PIPELINE MANAGER
 *
 *  Orchestrates all bucket io, caching, and pre/post-processing
 *
 */

// T-dataType, K-cacheKey, V-cacheValue
template<typename T, typename K, typename V>
class BucketedPipelineManager {
protected:
    // IO variables
    InterleavedIOScheduler<T> _io_subsystem;

    // Global cache variables
    // TODO implement cross-only compression for caching
    InMemCache<K, V> _cache_subsystem;

    // Effort limits
    uint64_t _max_bucket_size;

    // Current bucket data structures
    std::vector<T> _current_bucket;
    std::vector<T> _unique_entries;
    std::vector<std::pair<uint64_t, uint64_t> > _multiplexer; // file id, value lookup

    // Compression variables
    CompressionLevel _compression_level;
    std::unordered_map<K, std::pair<uint64_t, uint64_t> > _duplicate_finder;

    // State variables
    bool pipe_clear_flag; // previous bucket is ready to write

    // Locks for thread safety
    std::mutex _pipe_mutex;

    // Functors
    std::function<K(T)> extract_key; // extract key from data or transform data into key

    std::function<std::string(T, V)> postprocess_fn; // transform data and/or value to string for writing to file

public:
    /*
     * Constructors
     */

    BucketedPipelineManager();

    /*
     * Managed Operations
     */

    void open();

    std::shared_ptr<std::vector<T>> read();

    std::future<std::shared_ptr<std::vector<T> > > read_async();

    bool write(const std::vector<V> &out);

    std::future<bool> write_async(const std::vector<V> &out);

    void close();

    /*
     * State descriptors
     */

    bool empty() { return _io_subsystem.empty(); }

    uint64_t current_bucket_size() { return _current_bucket.size(); }

    uint64_t compressed_size() { return _unique_entries.size(); }

    uint64_t cache_hits() { return _cache_subsystem.hits(); }

    /*
     * Operator Overloads
     */

    friend std::ostream &operator<<(std::ostream &output, const BucketedPipelineManager &B) {
        output << *B._cache_subsystem << std::endl;
        return output;
    }
};

template<typename T, typename K, typename V>
BucketedPipelineManager<T, K, V>::BucketedPipelineManager() {
    _max_bucket_size = 50000;
    _compression_level = NONE;
    pipe_clear_flag = false;
    //TODO: initialize io and cache
}

template<typename T, typename K, typename V>
void BucketedPipelineManager<T, K, V>::open() {
    _io_subsystem.begin_reading();
    pipe_clear_flag = true;
}

template<typename T, typename K, typename V>
std::shared_ptr<std::vector<T>> BucketedPipelineManager<T, K, V>::read() {
    std::lock_guard<std::mutex> lock(_pipe_mutex);
    if (pipe_clear_flag) {
        // read next bucket after previous is written
        pipe_clear_flag = false;
        std::unique_ptr<std::vector<std::pair<uint64_t, T> > > next_bucket = _io_subsystem.request_bucket();

        // prepare to compress
        _current_bucket.resize(next_bucket->size());
        _multiplexer.reserve(next_bucket->size());
        // extract data (separate from file id) and compress
        uint64_t i = 0;
        for (const auto &mtpx_item : next_bucket.get()) {
            // extract data
            _current_bucket[i] = mtpx_item->second;

            if (_duplicate_finder.find(extract_key(_current_bucket[i])) != _duplicate_finder.end()) {
                // duplicate found, handle according to compression level
                switch (_compression_level) {
                    case NONE:
                        // no multiplexing, possibly cached
                        break;
                    case CROSS:
                        // only compress if the previous duplicate is not from the same file according to file id
                        if (_duplicate_finder.at(extract_key(_current_bucket[i])).first != mtpx_item.first) {
                            _multiplexer[i] = std::make_pair(mtpx_item->first,
                                                             _duplicate_finder.at(
                                                                     extract_key(_current_bucket[i]).second))
                        } else {
                            // otherwise count as a unique entry
                            _unique_entries.emplace_back(mtpx_item->second);
                            _multiplexer[i] = std::make_pair(mtpx_item->first, i);
                        }
                        break;
                    case FULL:
                        // compress if any duplication detected
                        _multiplexer[i] = std::make_pair(mtpx_item->first,
                                                         _duplicate_finder.at(extract_key(_current_bucket[i]).second));
                        break;
                    default:
                        break;
                }
            } else if (_cache_subsystem->find(extract_key(_current_bucket[i])) != _cache_subsystem->end()) {
                // if not duplicate but found in cache (or duplicate but all exist in cache), flag for lookup later
                _multiplexer[i] = std::make_pair(mtpx_item->first, UINT64_MAX);
            } else {
                // unique, non-cached value return as part of compressed bucket
                _unique_entries.emplace_back(mtpx_item->second);
                _multiplexer[i] = std::make_pair(mtpx_item->first, i);

                // store for later lookup to detect further duplicates
                if (_compression_level > NONE) {
                    _duplicate_finder.emplace(extract_key(_current_bucket[i]),
                                              std::make_pair(mtpx_item.first, _unique_entries.size() - 1);
                }
            }
            i++;
        }
        _duplicate_finder.clear();

        // return only the unique entries given the compression level
        return _unique_entries;
    } else {
        // TODO: throw more explicit exception
        return nullptr;
    }
}

template<typename T, typename K, typename V>
std::future<std::shared_ptr<std::vector<T> > > BucketedPipelineManager<T, K, V>::read_async() {
    std::future<std::shared_ptr<std::vector<T> > > future = std::async(std::launch::async, [&]() { return read(); });
    return future;
}

template<typename T, typename K, typename V>
bool BucketedPipelineManager<T, K, V>::write(const std::vector<V> &out) {
    std::lock_guard<std::mutex> lock(_pipe_mutex);
    if (!pipe_clear_flag) {
        // should have exactly as many unique enties as values
        assert(_unique_entries.size() == out.size());

        // de-multiplex using multiplexer built when reading
        for (uint64_t i = 0; i < _current_bucket.size(); i++) {
            if (_multiplexer[i].second == UINT64_MAX) {
                // found in cache earlier, report cached value
                _io_subsystem.write_async(_multiplexer[i].first, postprocess_fn(_current_bucket[i], _cache_subsystem.at(
                        extract_key(_current_bucket[i]))));
            } else {
                // otherwise, write value indicated by multiplexer
                _cache_subsystem->insert(extract_key(_current_bucket[i]), out[i]);
                _io_subsystem.write_async(_multiplexer[i].first,
                                          postprocess_fn(_current_bucket[i], out[_multiplexer[i].second]));
            }
        }

        // clear internal data structures and make ready for next reading
        _current_bucket.clear();
        _multiplexer.clear();
        _unique_entries.clear();
        pipe_clear_flag = true;
    }

    return pipe_clear_flag;
}

template<typename T, typename K, typename V>
std::future<bool> BucketedPipelineManager<T, K, V>::write_async(const std::vector<V> &out) {
    std::future<bool> future = std::async(std::launch::async, [&]() { return write(out); });
    return future;
}

template<typename T, typename K, typename V>
void BucketedPipelineManager<T, K, V>::close() {
    pipe_clear_flag = false;
    _io_subsystem.stop_reading();
    _io_subsystem.flush();
}

#endif //ALIGNER_CACHE_PIPELINE_HPP
