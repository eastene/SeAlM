//
// Created by evan on 4/22/19.
//

#ifndef ALIGNER_CACHE_TYPES_HPP
#define ALIGNER_CACHE_TYPES_HPP

#include <vector>
#include <string>
#include <chrono>
#include <variant>


struct CLIOptions {
    std::string input_file;
    std::string reference;
    std::string output_file;
    std::string metrics_file = "metrics.log";
    uint32_t batch_size = 100000;
    int manager_type = 0;
    int cache_type = 0;
    int verbose_flag = 0;
};


typedef std::vector<std::string> Read;
typedef std::chrono::milliseconds Mills;
typedef std::tuple<std::variant<std::string, uint32_t>, std::string, std::string> RedupeRef;

#endif //ALIGNER_CACHE_TYPES_HPP
