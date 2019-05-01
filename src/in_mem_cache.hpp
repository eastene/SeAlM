//
// Created by evan on 4/19/19.
//

#ifndef ALIGNER_CACHE_IN_MEM_CACHE_HPP
#define ALIGNER_CACHE_IN_MEM_CACHE_HPP

#include <iostream>
#include <string>
#include <list>
#include <vector>
#include <unordered_map>
#include "types.hpp"

namespace cache {

    class InMemCache {
    private:
        std::unordered_map<std::string, std::string> _cache;
    protected:
        uint32_t _max_cache_size; // num of elements
        // Metrics
        uint64_t _hits;
        uint64_t _misses;
        uint32_t _keys;
    public:

        InMemCache():_max_cache_size{900000},_hits{0},_misses{0},_keys{0}{};

        virtual void evict() = 0;

        virtual void add_batch(const std::vector<Read> &batch, const std::vector<std::string> &alignments) = 0;

        double get_hit_rate() {
            return _misses > 0 ? _hits / (_misses + _hits) : 0;
        }

        uint64_t get_hits() { return _hits; }

        virtual std::unordered_map<std::string, std::string>::iterator find(const Read &item) = 0;

        virtual std::unordered_map<std::string, std::string>::iterator end() = 0;

        virtual std::string &at(const Read &item) = 0;

        virtual std::string &operator[](Read &item) = 0;

        friend std::ostream &operator<<(std::ostream &output, const InMemCache &C) {
            output << "Hits: " << C._hits << " Misses: " << C._misses << " Size: " << C._keys;
            return output;
        }
    };

    class DummyCache : public InMemCache {
    private:
        std::unordered_map<std::string, std::string> _cache;

    public:

        DummyCache();

        void evict() override;

        void add_batch(const std::vector<Read> &batch, const std::vector<std::string> &alignments) override;

        std::unordered_map<std::string, std::string>::iterator find(const Read &item) override;

        std::unordered_map<std::string, std::string>::iterator end() override;

        std::string &at(const Read &item) override;

        std::string &operator[](Read &item) override;
    };


    class LRUCache : public InMemCache {
    protected:
        std::list<std::string> _order;
        std::unordered_map<std::string, std::list<std::string>::iterator> _order_lookup;
        std::unordered_map<std::string, std::string> _lru_cache;

        uint32_t _skip = 1;
    public:

        LRUCache();

        void evict() override;

        void add_batch(const std::vector<Read> &batch, const std::vector<std::string> &alignments) override;

        std::unordered_map<std::string, std::string>::iterator find(const Read &item) override;

        std::unordered_map<std::string, std::string>::iterator end() override;

        std::string &at(const Read &item) override;

        std::string &operator[](Read &item) override;
    };

    class MRUCache : public LRUCache {
        void evict() override;
    };
}

#endif //ALIGNER_CACHE_IN_MEM_CACHE_HPP
