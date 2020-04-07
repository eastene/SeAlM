//
// Created by evan on 4/19/19.
//

#ifndef ALIGNER_CACHE_MAPPING_UTILS_HPP
#define ALIGNER_CACHE_MAPPING_UTILS_HPP

#include <cstdio>
#include <vector>
#include <string>
#include <sstream>
#include "../lib/external/subprocess.hpp"
#include "wrapped_mapper.hpp"

double
align_batch(std::string &command, std::vector<Read> &batch, std::vector<std::string> *alignments) {
    std::stringstream ss;
    for (Read read : batch) {
        ss << read[0] << '\n';
        ss << read[1] << '\n';
        ss << read[2] << '\n';
        ss << read[3] << '\n';
    }

//    int inpipefd[2];
//    int outpipefd[2];
//    pid_t pid = -1;
//    pipe(inpipefd);
//    pipe(outpipefd);
//    pid = fork();
//    if (pid == 0) {
//        //close unused pipe end
//        close(outpipefd[0]);
//
//        // child, run outside process, redirect output to pipe
//        dup2(inpipefd[1], STDOUT_FILENO);
//        FILE *in = popen(command.c_str(), "w");
//        fputs(ss.str().c_str(), in);
//        exit(0);
//    }
//    //close unused pipe ends
//    close(outpipefd[0]);
//    close(inpipefd[1]);
//
//    char buf[1024];
//    uint32_t k = 0;
//    bool more_data = true;
//    while (read(inpipefd[0], buf, sizeof(buf)) && more_data) {
//        for (const auto c : buf) {
//            if (c == '\n') {
//                k++;
//            } else if (k >= batch.size()){
//                more_data = false;
//            } else {
//                (*alignments)[k].push_back(c);
//            }
//            //std::cout << buf;
//        }
//    }

    // TODO: find a more portable solution for calling aligner process
    auto proc = subprocess::Popen({command}, subprocess::input{subprocess::PIPE},
                                  subprocess::output{subprocess::PIPE},
                                  subprocess::error{subprocess::PIPE});

    long align_start = std::chrono::duration_cast<Mills>(
            std::chrono::system_clock::now().time_since_epoch()).count();

    auto res = proc.communicate(ss.str().c_str(), ss.str().size());
    uint64_t k = 0;

    std::stringstream out_ss;
    out_ss << res.first.buf.data();
    for (std::string line; std::getline(out_ss, line);) {
        (*alignments)[k++] = line;
    }

    long align_end = std::chrono::duration_cast<Mills>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    return (align_end - align_start) / 1000.00;
}

double call_aligner(std::string &command, std::vector<Read> &reduced_batch, std::vector<std::string> *alignments) {
    //alignments->clear();
    //alignments->resize(reduced_batch.size());

    return align_batch(command, reduced_batch, alignments);
}


#endif //ALIGNER_CACHE_MAPPING_UTILS_HPP
