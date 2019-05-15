//
// Created by evan on 5/9/19.
//

#ifndef ALIGNER_CACHE_PREP_EXPERIMENT_HPP
#define ALIGNER_CACHE_PREP_EXPERIMENT_HPP

#include "../lib/config.hpp"
#include "../lib/pipeline.hpp"

/*
 * Hash functions
 */


uint64_t single_hash(const std::pair<uint64_t, Read> &data) {
    auto payload = data.second;
    switch (payload[1][0]) {
        case 'A':
            return 0;
        case 'C':
            return 1;
        case 'T':
            return 2;
        case 'G':
            return 3;
        default:
            return 3;
    }
}

uint64_t double_hash(const std::pair<uint64_t, Read> &data) {
    auto payload = data.second;
    uint64_t hash = 0b0;
    switch (payload[1][0]) {
        case 'A':
            hash |= 0b00;
        case 'C':
            hash |= 0b01;
        case 'T':
            hash |= 0b10;
        case 'G':
            hash |= 0b11;
        default:
            hash |= 0b11;
    }

    switch (payload[1][1]) {
        case 'A':
            hash |= 0b0000;
        case 'C':
            hash |= 0b0100;
        case 'T':
            hash |= 0b1000;
        case 'G':
            hash |= 0b1100;
        default:
            hash |= 0b1100;
    }

    return hash;
}

uint64_t triple_hash(const std::pair<uint64_t, Read> &data) {
    auto payload = data.second;
    uint64_t hash = 0b0;
    switch (payload[1][0]) {
        case 'A':
            hash |= 0b00;
        case 'C':
            hash |= 0b01;
        case 'T':
            hash |= 0b10;
        case 'G':
            hash |= 0b11;
        default:
            hash |= 0b11;
    }

    switch (payload[1][1]) {
        case 'A':
            hash |= 0b0000;
        case 'C':
            hash |= 0b0100;
        case 'T':
            hash |= 0b1000;
        case 'G':
            hash |= 0b1100;
        default:
            hash |= 0b1100;
    }

    switch (payload[1][1]) {
        case 'A':
            hash |= 0b000000;
        case 'C':
            hash |= 0b010000;
        case 'T':
            hash |= 0b100000;
        case 'G':
            hash |= 0b110000;
        default:
            hash |= 0b110000;
    }

    return hash;
}

/*
 * Postprocessing functions
 */

std::string retag_posprocess_fn(Read &data, std::string &value) {
    std::stringstream ss;
    std::string tag = data[0].substr(1, data[0].find(' ') - 1);
    unsigned long sp1 = value.find('\t');
    // TODO: replace qual score with one from this read
    //unsigned long sp2 = alignment.find('\t', 9);
    std::string untagged = value.substr(sp1);

    ss << tag;
    ss << "\t";
    ss << untagged;

    return ss.str();
}

/*
 * Key Extraction Functions
 */

std::string seq_extraction_fn(Read &data) {
    return data[1];
}

// Configure the data pipeline appropriately according to the config file

// T-dataType, K-cacheKey, V-cacheValue
void prep_experiment(ConfigParser &cfp,  BucketedPipelineManager<Read, std::string, std::string> *pipe) {
    BufferedBuckets<std::pair<uint64_t, Read> > bb;
    InterleavedIOScheduler<Read> io;

    /*
     * Storage Parameters
     */

    if (cfp.contains("num_buckets"))
        bb.set_num_buckets(cfp.get_long_val("num_buckets"));

    if (cfp.contains("bucket_size"))
        bb.set_bucket_size(cfp.get_long_val("bucket_size"));

    if (cfp.contains("hash_func")) {
        std::string hash_func = cfp.get_val("hash_func");
        if (hash_func == "single") {
            bb.set_hash_fn(single_hash);
            bb.set_table_width(4);
        }
        else if (hash_func == "double") {
            bb.set_hash_fn(double_hash);
            bb.set_table_width(16);
        }
        else if (hash_func == "triple") {
            bb.set_hash_fn(double_hash);
            bb.set_table_width(64);
        }
        // else keep default
    }

    io.set_storage_subsystem(bb);

    /*
     * I/O Parameters
     */

    if (cfp.contains("async_io"))
        io.set_async_flag(cfp.get_bool_val("async_io"));

    if (cfp.contains("max_interleave"))
        io.set_max_interleave(cfp.get_long_val("max_interleave"));

    if (cfp.contains("input_pattern"))
        io.set_input_pattern(cfp.get_val("input_pattern"));

    if (cfp.contains("output_ext"))
        io.set_out_file_ext(cfp.get_val("output_ext"));

    if (cfp.contains("data_dir"))
        io.from_dir(cfp.get_val("data_dir"));

    pipe->set_io_subsystem(io);

    /*
     * Pipeline Parameters
     */

    if (cfp.contains("compression")){
        std::string cmp = cfp.get_val("compression");
        if (cmp == "full"){
            pipe->set_compression_level(FULL);
        } else if (cmp == "cross") {
            pipe->set_compression_level(CROSS);
        } else {
            pipe->set_compression_level(NONE);
        }
    }

    if (cfp.contains("cache")) {
        std::string cache = cfp.get_val("cache");
        if (cache == "lru") {
            LRUCache<std::string, std::string> c;
            pipe->set_cache_subsystem(c);
        } else if (cache == "mru") {
            MRUCache<std::string, std::string> c;
            pipe->set_cache_subsystem(c);
        }
    }

    if (cfp.contains("post_process_func")) {
        std::string post_proc = cfp.get_val("post_process_func");
        if (post_proc == "retag") {
            pipe->set_postprocess_fn(retag_posprocess_fn);
        }
    }

    pipe->set_extract_key_fn(seq_extraction_fn);

}

#endif //ALIGNER_CACHE_PREP_EXPERIMENT_HPP
