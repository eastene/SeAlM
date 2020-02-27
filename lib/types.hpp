//
// Created by evan on 4/22/19.
//

#ifndef ALIGNER_CACHE_TYPES_HPP
#define ALIGNER_CACHE_TYPES_HPP

#include <vector>
#include <string>
#include <chrono>


struct CLIOptions {
    std::string input_file_pattern = "*.fastq";
    std::string data_dir = "";
    std::string reference;
    std::string output_file;
    std::string metrics_file = "metrics.log";
    uint32_t batch_size = 50000;
    int manager_type = 0;
    int cache_type = 0;
    int verbose_flag = 0;
    int sam_suppress_flag = 0;
    int interleaved = 0;
    int threads = 3;
};


typedef std::vector<std::string> Read;
typedef std::chrono::milliseconds Mills;

#endif //ALIGNER_CACHE_TYPES_HPP
