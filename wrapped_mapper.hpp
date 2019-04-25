//
// Created by evan on 4/19/19.
//

#ifndef ALIGNER_CACHE_WRAPPED_MAPPER_HPP
#define ALIGNER_CACHE_WRAPPED_MAPPER_HPP


#include <string>
#include <set>
#include <assert.h>
#include <ctime>
#include <vector>
#include <fstream>
#include <chrono>

#include "in_mem_cache.hpp"
#include "batch_manager.hpp"
#include "types.hpp"

class WrappedMapper {
private:
    // extract necessary parameters
    std::string _input_file;
    std::string _reference;
    std::string _output_file;
    std::string _input_format;
    //std::set<std::string> formats {'.fastq', '.fasta', '.fa', '.fq'};
    //assert (['.fastq', '.fasta', '.fa', '.fq'])
    //assert (os.path.splitext(self._output_file)[1].lower() == '.sam')

    // extract extra parameters
    uint32_t _batch_size;
    uint32_t _qual_thresh;
    int _cache_type;
    int _manager_type;

    // derived parameters
    char _input_type; // if input_format in ['.fasta', '.fa'] else 'q'
    uint8_t _read_size;
    std::string _command;

    // batch manager with cache
    std::shared_ptr<batch::BatchManager> _batch_manager;

    // metrics
    double _total_time;
    double _align_time;
    uint64_t _reads_seen;
    uint64_t _reads_aligned;
    uint64_t _align_calls;
    std::vector<float> _throughput_vec;
    std::vector<int> _hits_vec;

public:
    WrappedMapper();

    void run_alignment();

    std::string prepare_log();

    friend std::ostream &operator<<(std::ostream &output, const WrappedMapper &W) {
        output << "Overall Runtime: " << W._total_time << "s\n";
        output << "  Total Align Time: " << W._align_time << "s\n";
        output << "Total reads " << W._reads_seen << "\n";
        output << "   Reads aligned " << W._reads_aligned << "\n";
        output << "Avg Throughput: " << (W._reads_seen / W._align_time) << " r/s\n";
        output << *W._batch_manager;

        return output;
    }

};


#endif //ALIGNER_CACHE_WRAPPED_MAPPER_HPP
