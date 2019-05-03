//
// Created by evan on 4/22/19.
//

#include <algorithm>
#include <cassert>
#include "batch_manager.hpp"

BatchManager::BatchManager(uint32_t batch_size, int cache_type) {
    _reduced_len = 0;
    _total_len = 0;
    _unique_batch.reserve(batch_size);
    _reduced_batch.resize(batch_size);
    _batch_size = batch_size;

    switch (cache_type) {
        case 0:
            std::cout << "Selecting no cache." << std::endl;
            _cache = std::make_shared<DummyCache<std::string, std::string> >();
            break;
        case 1:
            std::cout << "Selecting LRU cache." << std::endl;
            _cache = std::make_shared<LRUCache<std::string, std::string> >();
            break;
        case 2:
            std::cout << "Selecting MRU cache." << std::endl;
            _cache = std::make_shared<MRUCache<std::string, std::string> >();
            break;
        default:
            _cache = std::make_shared<DummyCache<std::string, std::string> >();
            break;
    }
}

void BatchManager::dedupe_batch(std::vector<Read> &batch) {
    _reduced_len = 0;
    _total_len = 0;
    _unique_batch.clear();
    //_batch.clear();

    std::string cached_alignment;
    uint32_t i = 0;
    for (const Read &read : batch) {
        if (_cache->find(read[1]) != _cache->end()) {
            cached_alignment = _cache->at(read[1]);
            _reduced_batch[i] = std::make_tuple(cached_alignment, read[0], read[3]);
        } else {
            _reduced_batch[i] = std::make_tuple(_reduced_len++, read[0], read[3]);
            _unique_batch.emplace_back(read);

        }
        i++;
    }
    if (i < _batch_size)
        _reduced_batch.resize(i);
    _total_len = batch.size();
}

void BatchManager::cache_batch(const std::vector<Read> &reduced_batch, const std::vector<std::string> &alignment) {
    assert(reduced_batch.size() == alignment.size());
    for(uint32_t i = 0; i < reduced_batch.size(); i++)
        _cache->insert(reduced_batch[i][1], alignment[i]);
}

void CompressedBatchManager::dedupe_batch(std::vector<Read> &batch) {
    _reduced_len = 0;
    _total_len = 0;
    _unique_batch.clear();
    _batch.clear();

    std::string cached_alignment;
    uint32_t i = 0;
    std::sort(batch.begin(), batch.end(), [](const Read& a, const Read& b) {
      return a[1] < b[1];
    });
    for (const Read &read : batch) {
        if (_cache->find(read[1]) != _cache->end()) {
            cached_alignment = _cache->at(read[1]);
            _reduced_batch[i] = std::make_tuple(cached_alignment, read[0], read[3]);
        } else if (_duplicate_finder.find(read[1]) == _duplicate_finder.end()) {
            _duplicate_finder.emplace(read[1], _reduced_len);
            _reduced_batch[i] = std::make_tuple(_reduced_len++, read[0], read[3]);
            _unique_batch.emplace_back(read);
        } else {
            _reduced_batch[i] = std::make_tuple(_duplicate_finder[read[1]], read[0], read[3]);
        }
        i++;
    }
    _total_len = batch.size();
}