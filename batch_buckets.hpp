//
// Created by evan on 4/30/19.
//

#ifndef ALIGNER_CACHE_BATCH_BUCKETS_HPP
#define ALIGNER_CACHE_BATCH_BUCKETS_HPP


#include <vector>
#include <list>
#include <memory>
#include "types.hpp"

class BatchBuckets {
private:
    uint16_t _num_buckets;
    uint32_t _max_bucket_size;
    uint64_t _size;
    std::vector<std::list<std::unique_ptr<std::vector<Read> > > >_buckets; // full buckets
    std::vector<std::unique_ptr<std::vector<Read> > > _buffers; // partially filled buckets
    std::vector<uint16_t> _chain_lengths;
    uint16_t _current_chain;
    std::list<std::unique_ptr<std::vector<Read> > >::iterator _next_bucket;

    uint64_t hash_func(const Read &read);

public:

    BatchBuckets(){
        _num_buckets = 4;
        _max_bucket_size = 50000;
        _size = 0;
        _buckets.resize(_num_buckets);
        _buffers.resize(_num_buckets);
        _chain_lengths.resize(_num_buckets);
        _current_chain = 0;
        _next_bucket = _buckets[0].end();
    }

    void insert(const Read &read);

    std::unique_ptr<std::vector<Read>> get_bucket();

    uint64_t size(){return _size;}
};


#endif //ALIGNER_CACHE_BATCH_BUCKETS_HPP
