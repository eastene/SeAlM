//
// Created by evan on 4/19/19.
//

#ifndef SEALM_MAPPING_UTILS_HPP
#define SEALM_MAPPING_UTILS_HPP

#include <cstdio>
#include <vector>
#include <string>
#include <sstream>
#include <cpp-subprocess/subprocess.hpp>
#include "wrapped_mapper.hpp"
#include "../lib/process.h"

class MapperProcess : public SubProccessAdapter {
protected:
    double
    align_batch(std::string &command, std::vector<Read> &batch, std::vector<std::string> *alignments) {
        std::stringstream ss;
        for (Read read : batch) {
            ss << read[0] << '\n';
            ss << read[1] << '\n';
            ss << read[2] << '\n';
            ss << read[3] << '\n';
        }

        popen(command);

        long align_start = std::chrono::duration_cast<Mills>(
                std::chrono::system_clock::now().time_since_epoch()).count();

        communicate_and_parse(ss, alignments);

        long align_end = std::chrono::duration_cast<Mills>(
                std::chrono::system_clock::now().time_since_epoch()).count();
        return (align_end - align_start) / 1000.00;
    }

public:
    double call_aligner(std::string &command, std::vector<Read> &reduced_batch, std::vector<std::string> *alignments) {
        return align_batch(command, reduced_batch, alignments);
    }
};


#endif //SEALM_MAPPING_UTILS_HPP
