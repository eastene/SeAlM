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

#include "../lib/cache.hpp"
#include "../lib/pipeline.hpp"
#include "types.hpp"
#include "../lib/io.hpp"
#include "../lib/config.hpp"

class WrappedMapper {
private:
    // extract necessary parameters
    std::vector<std::string> _input_files;
    std::string _reference;
    std::vector<std::string> _output_files;
    std::string _input_format;
    //std::set<std::string> formats {'.fastq', '.fasta', '.fa', '.fq'};
    //assert (['.fastq', '.fasta', '.fa', '.fq'])
    //assert (os.path.splitext(self._output_file)[1].lower() == '.sam')

    // extract extra parameters
    uint32_t _bucket_size;
    uint32_t _qual_thresh;
    int _cache_type;
    int _manager_type;

    // derived parameters
    char _input_type; // if input_format in ['.fasta', '.fa'] else 'q'
    uint8_t _read_size;
    std::string _command;

    // Pipeline manager
    PipelineParams _params;
    BucketedPipelineManager<Read, std::string, std::string> _pipe;

    // metrics
    double _total_time;
    double _align_time;
    uint64_t _reads_seen;
    uint64_t _reads_aligned;
    uint64_t _align_calls;
    std::vector<float> _throughput_vec;
    std::vector<int> _hits_vec;
    std::vector<float> _batch_time_vec;
    std::vector<uint32_t> _reads_aligned_vec;

    // private methods
    void initialize_alignment();

public:
    explicit WrappedMapper(CLIOptions &opts);

    explicit WrappedMapper(ConfigParser &configs);

    void run_alignment();

    std::string prepare_log();

    friend std::ostream &operator<<(std::ostream &output, const WrappedMapper &W) {
        output << "Overall Runtime: " << W._total_time << "s\n";
        output << "  Total Align Time: " << W._align_time << "s\n";
        output << "Total reads " << W._reads_seen << "\n";
        output << "   Reads aligned " << W._reads_aligned << "\n";
        output << "Avg Throughput: " << (W._reads_seen / W._align_time) << " r/s\n";
//        output << *W._batch_manager;

        return output;
    }

};


#endif //ALIGNER_CACHE_WRAPPED_MAPPER_HPP
