//
// Created by evan on 4/22/19.
//

#ifndef ALIGNER_CACHE_PIPELINE_HPP
#define ALIGNER_CACHE_PIPELINE_HPP

#include <variant>
#include <memory>
#include <queue>
#include "../src/types.hpp"
#include "cache.hpp"
#include "io.hpp"

struct PipelineParams {
    std::string input_file_pattern = "";
    std::string data_dir = std::experimental::filesystem::current_path();
};

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
class BucketedPipelineManager : public Observable {
protected:
    // IO variables
    //TODO: change to unique pointer (?)
    InterleavedIOScheduler<T> _io_subsystem;

    // Global cache variables
    // TODO implement cross-only compression for caching
    std::shared_ptr<InMemCache<K, V> > _cache_subsystem;

    // Current bucket data structures
    std::vector<T> _current_bucket;
    std::vector<T> _unique_entries;
    std::vector<std::pair<uint64_t, uint64_t> > _multiplexer; // file id, value lookup

    // Lock free I/O buffers
    std::queue<std::unique_ptr<std::vector<T> > > _bucket_buffer;
    std::queue<std::unique_ptr<std::vector<std::pair<uint64_t, uint64_t> > > > _multiplexer_buffer;
    std::queue<std::unique_ptr<std::queue<V> > > _cached_values;
    std::queue<uint64_t> _prev_bucket_sizes;
    std::queue<double> _prev_compression_ratios;

    // Compression variables
    CompressionLevel _compression_level;
    std::unordered_map<K, std::pair<uint64_t, uint64_t> > _duplicate_finder;

    // State variables
    std::atomic<bool> _pipe_clear_flag; // previous bucket is ready to write

    // Locks for thread safety
    std::mutex _pipe_mutex;

    // Functors
    std::function<K(T &)> _extract_key_fn; // extract key from data or transform data into key

    std::function<std::string(T &, V &)> _postprocess_fn; // transform data and/or value to string for writing to file

public:
    /*
     * Constructors
     */

    BucketedPipelineManager();

    /*
     * Subprocess Initialization
     */

    void set_params(PipelineParams &params);

    /*
     * Managed Operations
     */

    void open();

    std::vector<T> read();

    std::vector<T> lock_free_read();

    std::future<std::vector<T> > read_async();

    bool write(std::vector<V> &out);

    bool lock_free_write(std::vector<V> &out);

    std::future<bool> write_async(std::vector<V> &out);

    void close();

    /*
     * Getters/Setters
     */

    std::vector<std::string> get_filenames() { return _io_subsystem.get_input_filenames(); }

    void set_compression_level(CompressionLevel cl) { _compression_level = cl; }

    void set_cache_subsystem(std::shared_ptr<InMemCache<K, V> > &other) { _cache_subsystem = other; }

    void set_io_subsystem(InterleavedIOScheduler<T> &other) { _io_subsystem = other; }

    void set_postprocess_fn(std::function<std::string(T &, V &)> fn) { _postprocess_fn = fn; }

    void set_extract_key_fn(std::function<K(T &)> fn) { _extract_key_fn = fn; }

    /*
     * State Descriptors
     */

    bool empty() { return _io_subsystem.empty(); }

    bool full() { return _io_subsystem.full(); }

    uint64_t current_bucket_size() {
        uint64_t tmp = 0;
        if (!_prev_bucket_sizes.empty()) {
            tmp = _prev_bucket_sizes.front();
            _prev_bucket_sizes.pop();
        }
        return tmp;
    }

    double current_compression_ratio() {
        double tmp = 0;
        if (!_prev_compression_ratios.empty()) {
            tmp = _prev_compression_ratios.front();
            _prev_compression_ratios.pop();
        }
        return tmp;
    }

    uint64_t compressed_size() { return _unique_entries.size(); }

    uint64_t cache_hits() { return _cache_subsystem->hits(); }

    uint64_t cache_misses() { return _cache_subsystem->misses(); }

    uint64_t capacity() { return _io_subsystem.capacity(); }

    /*
     * Operator Overloads
     */

    friend std::ostream &operator<<(std::ostream &output, const BucketedPipelineManager &B) {
        output << *B._cache_subsystem << std::endl;
        return output;
    }

    BucketedPipelineManager &operator=(const BucketedPipelineManager &other);
};

/*
 * Example Functors
 */

template<typename T, typename K, typename V>
std::string default_postprocess_fn(T &data, V &value) {
    return value;
}

template<typename T, typename K, typename V>
K default_extraction_fn(T &data) {
    return data[0];
}

/*
 * Method Implementations
 */

template<typename T, typename K, typename V>
BucketedPipelineManager<T, K, V>::BucketedPipelineManager() {
    _compression_level = NONE;
    _pipe_clear_flag = false;
    _postprocess_fn = std::function<std::string(T &, V &)>(
            [&](T &data, V &value) { return default_postprocess_fn<T, K, V>(data, value); });
    _extract_key_fn = std::function<K(T &)>([&](T &data) { return default_extraction_fn<T, K, V>(data); });
    _cache_subsystem = std::make_unique<DummyCache<K, V> >();
}

template<typename T, typename K, typename V>
void BucketedPipelineManager<T, K, V>::set_params(PipelineParams &params) {
    _io_subsystem.set_input_pattern(params.input_file_pattern);
    _io_subsystem.from_dir(params.data_dir);
}

template<typename T, typename K, typename V>
void BucketedPipelineManager<T, K, V>::open() {
    _io_subsystem.begin_reading();
    _pipe_clear_flag = true;
}

template<typename T, typename K, typename V>
std::vector<T> BucketedPipelineManager<T, K, V>::read() {
    std::lock_guard<std::mutex> lock(_pipe_mutex);
    if (_pipe_clear_flag) {
        // read next bucket after previous is written
        _pipe_clear_flag = false;
        auto next_bucket = _io_subsystem.request_bucket();

        // prepare to compress
        _current_bucket.resize(next_bucket->size());
        _multiplexer.resize(next_bucket->size());
        // extract data (separate from file id) and compress
        uint64_t i = 0;
        for (const auto &mtpx_item : *next_bucket) {
            // extract data
            _current_bucket[i] = mtpx_item.second;

            if (_duplicate_finder.find(_extract_key_fn(_current_bucket[i])) != _duplicate_finder.end()) {
                // duplicate found, handle according to compression level
                switch (_compression_level) {
                    case NONE:
                        // no multiplexing, possibly cached
                        break;
                    case CROSS:
                        // only compress if the previous duplicate is not from the same file according to file id
                        if (_duplicate_finder.at(_extract_key_fn(_current_bucket[i])).first != mtpx_item.first) {
                            _multiplexer[i] = std::make_pair(mtpx_item.first,
                                                             _duplicate_finder.at(
                                                                     _extract_key_fn(_current_bucket[i])).second);
                        } else {
                            // otherwise count as a unique entry
                            _unique_entries.emplace_back(mtpx_item.second);
                            _multiplexer[i] = std::make_pair(mtpx_item.first, i);
                        }
                        break;
                    case FULL:
                        // compress if any duplication detected
                        _multiplexer[i] = std::make_pair(mtpx_item.first,
                                                         _duplicate_finder.at(
                                                                 _extract_key_fn(_current_bucket[i])).second);
                        break;
                    default:
                        break;
                }
            } else if (_cache_subsystem->find(_extract_key_fn(_current_bucket[i])) != _cache_subsystem->end()) {
                // if not duplicate but found in cache (or duplicate but all exist in cache), flag for lookup later
                _multiplexer[i] = std::make_pair(mtpx_item.first, UINT64_MAX);
            } else {
                // unique, non-cached value return as part of compressed bucket
                _unique_entries.emplace_back(mtpx_item.second);
                _multiplexer[i] = std::make_pair(mtpx_item.first, _unique_entries.size() - 1);

                // store for later lookup to detect further duplicates
                if (_compression_level > NONE) {
                    _duplicate_finder.emplace(_extract_key_fn(_current_bucket[i]),
                                              std::make_pair(mtpx_item.first, _unique_entries.size() - 1));
                }
            }
            i++;
        }
        _duplicate_finder.clear();

        // return only the unique entries given the compression level
        return _unique_entries;
    }
//    else {
//        // TODO: throw more explicit exception
//        return nullptr;
//    }
}

template<typename T, typename K, typename V>
std::vector<T> BucketedPipelineManager<T, K, V>::lock_free_read() {
    // get next available bucket
    auto next_bucket = _io_subsystem.request_bucket();

    // prepare to compress
    std::unordered_map<K, std::pair<uint64_t, uint64_t> > duplicate_finder;
    std::vector<T> unique_entries;
    auto temp_bucket = std::make_unique<std::vector<T> >();
    auto temp_multiplexer = std::make_unique<std::vector<std::pair<uint64_t, uint64_t>>>();
    auto temp_cache_hits = std::make_unique<std::queue<V> >();

    temp_bucket->resize(next_bucket->size());
    temp_multiplexer->resize(next_bucket->size());

    uint64_t i = 0;
    if (_compression_level == CompressionLevel::NONE){
        for (const auto &mtpx_item : *next_bucket) {
            // extract data
            (*temp_bucket)[i] = mtpx_item.second;

            if (_cache_subsystem->find(_extract_key_fn((*temp_bucket)[i])) != _cache_subsystem->end()) {
                // TODO make this atomic
                // if not duplicate but found in cache (or duplicate but all exist in cache), flag for lookup later
                (*temp_multiplexer)[i] = std::make_pair(mtpx_item.first, UINT64_MAX);
                temp_cache_hits->push(_cache_subsystem->at(_extract_key_fn((*temp_bucket)[i])));
            } else {
                // unique, non-cached value return as part of compressed bucket
                unique_entries.emplace_back(mtpx_item.second);
                (*temp_multiplexer)[i] = std::make_pair(mtpx_item.first, unique_entries.size() - 1);
            }
            i++;
        }
    } else {
        // extract data (separate from file id) and compress

        // TODO add locking for cache lookup and inserting
        for (const auto &mtpx_item : *next_bucket) {
            // extract data
            (*temp_bucket)[i] = mtpx_item.second;

            if (duplicate_finder.find(_extract_key_fn((*temp_bucket)[i])) != duplicate_finder.end()) {
                // duplicate found, handle according to compression level
                switch (_compression_level) {
                    case CompressionLevel::CROSS:
                        // only compress if the previous duplicate is not from the same file according to file id
                        if (duplicate_finder.at(_extract_key_fn((*temp_bucket)[i])).first != mtpx_item.first) {
                            (*temp_multiplexer)[i] = std::make_pair(mtpx_item.first,
                                                                    duplicate_finder.at(
                                                                            _extract_key_fn((*temp_bucket)[i])).second);
                        } else {
                            // otherwise count as a unique entry
                            unique_entries.emplace_back(mtpx_item.second);
                            (*temp_multiplexer)[i] = std::make_pair(mtpx_item.first, i);
                        }
                        break;
                    case CompressionLevel::FULL:
                        // compress if any duplication detected
                        (*temp_multiplexer)[i] = std::make_pair(mtpx_item.first,
                                                                duplicate_finder.at(
                                                                        _extract_key_fn((*temp_bucket)[i])).second);
                        break;
                    default:
                        break;
                }
            } else if (_cache_subsystem->find(_extract_key_fn((*temp_bucket)[i])) != _cache_subsystem->end()) {
                // TODO make this atomic
                // if not duplicate but found in cache (or duplicate but all exist in cache), flag for lookup later
                (*temp_multiplexer)[i] = std::make_pair(mtpx_item.first, UINT64_MAX);
                temp_cache_hits->push(_cache_subsystem->at(_extract_key_fn((*temp_bucket)[i])));
            } else {
                // unique, non-cached value return as part of compressed bucket
                unique_entries.emplace_back(mtpx_item.second);
                (*temp_multiplexer)[i] = std::make_pair(mtpx_item.first, unique_entries.size() - 1);

                // store for later lookup to detect further duplicates
                if (_compression_level > NONE) {
                    duplicate_finder.emplace(_extract_key_fn((*temp_bucket)[i]),
                                             std::make_pair(mtpx_item.first, unique_entries.size() - 1));
                }
            }
            i++;
        }
    }

    // buffer inputs for lock free writing
    _bucket_buffer.emplace(std::move(temp_bucket));
    _multiplexer_buffer.emplace(std::move(temp_multiplexer));
    _cached_values.emplace(std::move(temp_cache_hits));
    _prev_bucket_sizes.emplace(next_bucket->size());
    _prev_compression_ratios.emplace(next_bucket->size() / static_cast<double>(unique_entries.size()));

    // return only the unique entries given the compression level
    return unique_entries;

}

template<typename T, typename K, typename V>
std::future<std::vector<T> > BucketedPipelineManager<T, K, V>::read_async() {
    auto future = std::async(std::launch::async, [&]() { return this->lock_free_read(); });
    return future;
}

template<typename T, typename K, typename V>
bool BucketedPipelineManager<T, K, V>::write(std::vector<V> &out) {
    std::lock_guard<std::mutex> lock(_pipe_mutex);
    if (!_pipe_clear_flag) {
        std::string line_out;
        // should have exactly as many unique enties as values
        assert(_unique_entries.size() == out.size());
        // de-multiplex using multiplexer built when reading
        for (uint64_t i = 0; i < _current_bucket.size(); i++) {
            if (_multiplexer[i].second == UINT64_MAX) {
                // found in cache earlier, report cached value
                line_out = _postprocess_fn(_current_bucket[i],
                                           _cache_subsystem->at(_extract_key_fn(_current_bucket[i])));
                _io_subsystem.write_async(_multiplexer[i].first, line_out);
            } else {
                // otherwise, write value indicated by multiplexer
                line_out = _postprocess_fn(_current_bucket[i], out[_multiplexer[i].second]);
                _cache_subsystem->insert_no_evict(_extract_key_fn(_current_bucket[i]), out[_multiplexer[i].second]);
                _io_subsystem.write_async(_multiplexer[i].first, line_out);
            }
        }
        _cache_subsystem->trim();

        // clear internal data structures and make ready for next reading
        _current_bucket.clear();
        _multiplexer.clear();
        _unique_entries.clear();
        _pipe_clear_flag = true;
    }

    return _pipe_clear_flag;
}

template<typename T, typename K, typename V>
bool BucketedPipelineManager<T, K, V>::lock_free_write(std::vector<V> &out) {
    if (!_bucket_buffer.empty()) {
        auto temp_bucket = std::move(_bucket_buffer.front());
        _bucket_buffer.pop();
        auto temp_multiplexer = std::move(_multiplexer_buffer.front());
        _multiplexer_buffer.pop();
        auto temp_cache_hits = std::move(_cached_values.front());
        _cached_values.pop();

        std::string line_out;
        // should have exactly as many unique enties as values
        // assert(_unique_entries.size() == out.size()); have to ignore for lock free
        // de-multiplex using multiplexer built when reading
        long w_start = std::chrono::duration_cast<Mills>(std::chrono::system_clock::now().time_since_epoch()).count();
        for (uint64_t i = 0; i < temp_bucket->size(); i++) {
            if ((*temp_multiplexer)[i].second == UINT64_MAX) {
                // found in cache earlier, report cached value
                line_out = _postprocess_fn((*temp_bucket)[i], temp_cache_hits->front());
                temp_cache_hits->pop();
                _io_subsystem.write_async((*temp_multiplexer)[i].first, line_out);
            } else {
                // otherwise, write value indicated by multiplexer
                line_out = _postprocess_fn((*temp_bucket)[i], out[(*temp_multiplexer)[i].second]);
                _io_subsystem.write_async((*temp_multiplexer)[i].first, line_out);
                // TODO: figure out why this is so slow
                _cache_subsystem->insert(_extract_key_fn((*temp_bucket)[i]), out[(*temp_multiplexer)[i].second]);
            }
        }
        notify(1);
        //_cache_subsystem->trim();
        long w_end = std::chrono::duration_cast<Mills>(std::chrono::system_clock::now().time_since_epoch()).count();
        std::cout << "Write time: " << (w_end - w_start) << std::endl;
    }
}

template<typename T, typename K, typename V>
std::future<bool> BucketedPipelineManager<T, K, V>::write_async(std::vector<V> &out) {
    std::future<bool> future = std::async(std::launch::async, [&]() { return this->lock_free_write(out); });
    return future;
}

template<typename T, typename K, typename V>
void BucketedPipelineManager<T, K, V>::close() {
    _pipe_clear_flag = false;
    _io_subsystem.stop_reading();
    _io_subsystem.flush();
}

template<typename T, typename K, typename V>
BucketedPipelineManager<T, K, V> &BucketedPipelineManager<T, K, V>::operator=(const BucketedPipelineManager &other) {
    // IO variables
    _io_subsystem = other._io_subsystem;

    // Global cache variables
    // TODO implement cross-only compression for caching
    _cache_subsystem = other._cache_subsystem;

    // Current bucket data structures
    _current_bucket = other._current_bucket;
    _unique_entries = other._unique_entries;
    _multiplexer = other._multiplexer;

    // Lock free I/O buffers
    _bucket_buffer = other._bucket_buffer;
    _multiplexer_buffer = other._multiplexer_buffer;
    _cached_values = other._cached_values;

    // Compression variables
    _compression_level = other._compression_level;
    _duplicate_finder = other._duplicate_finder;

    // State variables
    _pipe_clear_flag = other._pipe_clear_flag; // previous bucket is ready to write

    // Locks for thread safety
    _pipe_mutex = other._pipe_mutex;

    // Functors
    _extract_key_fn = other._extract_key_fn;
    _postprocess_fn = other._postprocess_fn;
}

#endif //ALIGNER_CACHE_PIPELINE_HPP
