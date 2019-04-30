//
// Created by evan on 4/30/19.
//

#include <algorithm>
#include "batch_buckets.hpp"

uint64_t BatchBuckets::hash_func(const Read &read) {
    switch (read[1][0]) {
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

void BatchBuckets::insert(const Read &read) {
    uint64_t i = hash_func(read);
    _buffers[i]->emplace_back(read);

    if (_buffers[i]->size() >= _max_bucket_size) {
        _buckets[i].emplace_front(std::move(_buffers[i]));
        _buffers[i] = std::make_unique<std::vector<Read>>();
        _chain_lengths[i] = _buckets[i].size();
    }

    _size++;
}

std::unique_ptr<std::vector<Read>> BatchBuckets::get_bucket() {
    std::unique_ptr<std::vector<Read>> out = std::move(*_next_bucket);
    _buckets[_current_chain].pop_back();
    if (_buckets[_current_chain].empty()) {
        _current_chain = *std::max_element(_chain_lengths.begin(), _chain_lengths.end());
    }
    _next_bucket = _buckets[_current_chain].end();
    _size -= out->size();
    return out;
}