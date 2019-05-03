//
// Created by evan on 4/19/19.
//

#ifndef ALIGNER_CACHE_UTILS_HPP
#define ALIGNER_CACHE_UTILS_HPP

#include <cstdio>
#include <vector>
#include <string>
#include <sstream>
#include <cpp-subprocess/subprocess.hpp>
#include "wrapped_mapper.hpp"



uint32_t
next_batch(std::string &infile,
           uint32_t batch_size,
           std::vector<Read> *in_buffer,
           std::shared_ptr<BucketManager> batch_manager,
           bool *more_data,
           uint64_t *seek_pos) {

    uint32_t reads_seen = 0;
    std::ifstream fin(infile);
    fin.seekg(*seek_pos);
    // read in next batch
    for (uint32_t i = 0; i < batch_size; i++) {
        std::getline(fin, (*in_buffer)[i][0]);
        std::getline(fin, (*in_buffer)[i][1]);
        std::getline(fin, (*in_buffer)[i][2]);
        std::getline(fin, (*in_buffer)[i][3]);
        if (fin.eof()) {
            *more_data = false;
            break;
        }
        reads_seen++;
    }

    // search cache and remove remaining duplicates from batch
    batch_manager->dedupe_batch(*in_buffer);

    *seek_pos = fin.tellg();
    fin.close();
    return reads_seen;
}

void
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
    auto obuf = proc.communicate(ss.str().c_str(), ss.str().size()).first;
    int k = 0;
    for (const auto &c : obuf.buf) {
        if (c == '\n')
            k++;
        else
            (*alignments)[k].push_back(c);
    }
}

std::string retag(const std::string &alignment, const std::string &header, const std::string &qual) {
    char out[500];
    std::string tag = header.substr(1, header.find(' ') - 1);
    unsigned long sp1 = alignment.find('\t');
    // TODO: replace qual score with one from this read
    //unsigned long sp2 = alignment.find('\t', 9);
    std::string untagged = alignment.substr(sp1);
    sprintf(out, "%s\t%s", tag.c_str(), untagged.c_str());

    return std::string(out);
}

void write_batch(const std::string &output_file, const std::vector<RedupeRef> batch,
                 const std::vector<std::string> alignments) {
    std::ofstream fout;
    fout.open(output_file, std::ios::app);
    if (alignments.size() < batch.size()) {
        for (const auto &reduc_read : batch) {
            if (std::holds_alternative<uint32_t>(std::get<0>(reduc_read))) {
                fout << retag(alignments[std::get<uint32_t>(std::get<0>(reduc_read))], std::get<1>(reduc_read),
                              std::get<2>(reduc_read)) << "\n";
            } else {
                fout << retag(std::get<std::string>(std::get<0>(reduc_read)), std::get<1>(reduc_read),
                              std::get<2>(reduc_read)) << "\tC\n";
            }
        }
    } else {
        for (const auto &alignment : alignments) {
            fout << alignment << "\n";
        }
    }
    fout.close();
}

void align(std::string &command, std::vector<Read> reduced_batch, std::vector<std::string> *alignments) {
    alignments->clear();
    alignments->resize(reduced_batch.size());
    align_batch(command, reduced_batch, alignments);
}

#endif //ALIGNER_CACHE_UTILS_HPP