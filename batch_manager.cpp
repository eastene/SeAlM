//
// Created by evan on 4/22/19.
//

#include "batch_manager.hpp"

batch::BatchManager::BatchManager(uint32_t batch_size, int cache_type) {
    _reduced_len = 0;
    _total_len = 0;
    _reduced_buff.reserve(batch_size);
    _batch.resize(batch_size);
    _batch_size = batch_size;

    switch (cache_type) {
        case 0:
            std::cout << "Selecting no cache." << std::endl;
            _cache = std::make_shared<cache::DummyCache>();
            break;
        case 1:
            std::cout << "Selecting LRU cache." << std::endl;
            _cache = std::make_shared<cache::LRUCache>();
            break;
        case 2:
            std::cout << "Selecting MRU cache." << std::endl;
            _cache = std::make_shared<cache::MRUCache>();
            break;
        default:
            _cache = std::make_shared<cache::DummyCache>();
            break;
    }
}

void batch::BatchManager::dedupe_batch(std::vector<Read> &batch) {
    _reduced_len = 0;
    _total_len = 0;
    _reduced_buff.clear();
    //_batch.clear();

    std::string cached_alignment;
    uint32_t i = 0;
    for (const Read &read : batch) {
        if (_cache->find(read) != _cache->end()) {
            cached_alignment = _cache->at(read);
            _batch[i] = std::make_tuple(cached_alignment, read[0], read[3]);
        } else {
            _batch[i] = std::make_tuple(_reduced_len++, read[0], read[3]);
            _reduced_buff.emplace_back(read);

        }
        i++;
    }
    if (i < _batch_size)
        _batch.resize(i);
    _total_len = batch.size();
}

void batch::BatchManager::cache_batch(const std::vector<Read> &reduced_batch, const std::vector<std::string> &alignment) {
    _cache->add_batch(reduced_batch, alignment);
}

void batch::CompressedBatchManager::dedupe_batch(std::vector<Read> &batch) {
    _reduced_len = 0;
    _total_len = 0;
    _reduced_buff.clear();
    _batch.clear();

    std::string cached_alignment;
    uint32_t i = 0;
    for (const Read &read : batch) {
        if (_cache->find(read) != _cache->end()) {
            cached_alignment = _cache->at(read);
            _batch[i] = std::make_tuple(cached_alignment, read[0], read[3]);
        } else if (_duplicate_finder.find(read[1]) == _duplicate_finder.end()) {
            _duplicate_finder.emplace(read[1], _reduced_len);
            _batch[i] = std::make_tuple(_reduced_len++, read[0], read[3]);
            _reduced_buff.emplace_back(read);
        } else {
            _batch[i] = std::make_tuple(_duplicate_finder[read[1]], read[0], read[3]);
        }
        i++;
    }
    _total_len = batch.size();
}