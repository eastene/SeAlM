//
// Created by evan on 4/22/19.
//

#ifndef ALIGNER_CACHE_BATCH_MANAGER_HPP
#define ALIGNER_CACHE_BATCH_MANAGER_HPP

#include <variant>
#include <memory>
#include "types.hpp"
#include "../lib/cache.hpp"


/*
 *
 *  BATCH MANAGER
 *
 *  Constructs a batch, using the global cache when necessary to
 *  reuse existing alignments. (Using with standard InMemCache
 *  will remove all forms of caching)
 *
 */

// T-dataType, V-cacheValue, C-Cache
template<typename T, typename V, typename C>
class BatchManager {
protected:
    std::shared_ptr<C> _cache;
    uint32_t _reduced_len;
    uint32_t _total_len;
    uint32_t _batch_size;
    std::vector<T> _unique_batch;
    std::vector<RedupeRef> _reduced_batch;

public:
    /*
     * Constructors
     */

    BatchManager() : _reduced_len{0}, _total_len{0}, _batch_size{50000} {};

    explicit BatchManager(uint32_t batch_size);

    /*
     * Local Cache Operations
     */

    virtual void dedupe_batch(std::vector<T> &batch);

    /*
     * Global Cache Operations
     */

    void cache_batch(const std::vector<T> &reduced_batch, const std::vector<V> &alignment);

    uint64_t get_hits() { return _cache->hits(); }

    /*
     * Getters/Setters
     */

    std::vector<T> &get_unique_batch() { return _unique_batch; }

    std::vector<RedupeRef> &get_reduced_batch() { return _reduced_batch; }

    /*
     * Operator Overloads
     */

    friend std::ostream &operator<<(std::ostream &output, const BatchManager &B) {
        output << *B._cache << std::endl;
        return output;
    }
};

/*
 *
 * COMPRESSED BATCH MANAGER
 *
 * Removes duplicates from a batch before alignment and provides the
 * information necessary to reconstruct the full batch in order.
 *
 */

class CompressedBatchManager : public BatchManager {
private:
    /*
     * Hash map used to identify duplicates within a batch
     */
    std::unordered_map<std::string, uint32_t> _duplicate_finder;

public:
    /*
     * Constructors
     */

    CompressedBatchManager() = default;

    explicit CompressedBatchManager(uint32_t batch_size, int cache_type) : BatchManager(batch_size, cache_type) {
        _duplicate_finder.reserve(batch_size);
    }

    /*
     * Local Cache Operations
     */

    void dedupe_batch(std::vector<Read> &batch) override;
};


#endif //ALIGNER_CACHE_BATCH_MANAGER_HPP
