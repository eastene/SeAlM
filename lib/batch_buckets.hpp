//
// Created by evan on 4/30/19.
//

#ifndef ALIGNER_CACHE_BATCH_BUCKETS_HPP
#define ALIGNER_CACHE_BATCH_BUCKETS_HPP


#include <vector>
#include <list>
#include <memory>

template <typename T>
class BatchBuckets {
private:
    // sizing limits
    uint16_t _max_buckets;
    uint32_t _max_bucket_size;

    // state descriptors
    uint64_t _num_buckets;
    uint64_t _size;
    std::vector<uint16_t> _chain_lengths;
    uint16_t _current_chain;
    typename std::list<std::unique_ptr<std::vector<T> > >::iterator _next_bucket;

    // data structures
    std::vector<std::list<std::unique_ptr<std::vector<T> > > >_buckets; // full buckets
    std::vector<std::unique_ptr<std::vector<T> > > _buffers; // partially filled buckets

    // private methods
    uint16_t hash_func(const T &data);
    std::unique_ptr<std::vector<T>> get_bucket();

public:

    BatchBuckets(){
        _max_buckets = 4;
        _max_bucket_size = 50000;
        _size = 0;
        _num_buckets = 0;
        _buckets.resize(_max_buckets);
        _buffers.resize(_max_buckets);
        _chain_lengths.resize(_max_buckets);
        _current_chain = 0;
        _next_bucket = _buckets[0].end();
    }

    void insert(const T &data);

    std::unique_ptr<std::vector<T>> wait_for_bucket();

    uint64_t size(){return _size;}
};


#endif //ALIGNER_CACHE_BATCH_BUCKETS_HPP
