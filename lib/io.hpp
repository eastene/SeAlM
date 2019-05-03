//
// Created by Evan Stene on 2019-05-02.
//

#ifndef ALIGNER_CACHE_IO_HPP
#define ALIGNER_CACHE_IO_HPP

#include <vector>
#include <cstdint>

// T-dataType, S-Storage, M-Manager
template <typename T, typename S, typename M>
class BufferedIOScheduler{
private:
    // Effort limits
    uint16_t _max_io_interleave;

    // Private methods
    std::vector<T> parse_fn();

public:
    BufferedIOScheduler<T,S,M>():_max_io_interleave{10}{}

    bool begin_reading();

    std::vector<T> request_batch();

    bool stop_reading();
};

template <typename T, typename S, typename M, typename C>
std::vector<T> BufferedIOScheduler<T,S,M,C>::parse_fn() {
    for ()
}

#endif //ALIGNER_CACHE_IO_HPP
