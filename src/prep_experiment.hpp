//
// Created by evan on 5/9/19.
//

#ifndef SEALM_PREP_EXPERIMENT_HPP
#define SEALM_PREP_EXPERIMENT_HPP

#include "../lib/config.hpp"
#include "../lib/pipeline.hpp"

/*
 * READ ORDERING
 */
struct ReadOdering : public ValueOrdering< std::pair<uint64_t, Read> > {
    bool operator () (const std::pair<uint64_t, Read> &l, const std::pair<uint64_t, Read> &r) {
        return l.second[1] < r.second[1];
    }
};

/*
 * PREFIX HASHING
 */

class NOPHasher : public DataHasher< std::pair<uint64_t, Read> >{
public:

    uint64_t _hash_fn(const std::pair<uint64_t, Read> &data) override {
        return 0;
    }

    uint64_t _required_table_width() override { return 1; }
};

class PrefixHasher : public DataHasher< std::pair<uint64_t, Read> >{
public:

    uint64_t _hash_fn(const std::pair<uint64_t, Read> &data) override {
        switch (data.second[1][0]) {
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

    uint64_t _required_table_width() override { return 4; }
};

class DoublePrefixHasher : public PrefixHasher {
public:
    uint64_t _hash_fn(const std::pair<uint64_t, Read> &data) final {
        uint64_t hash = 0;
        switch (data.second[1][1]) {
            case 'A':
                hash |= 0b00;
                break;
            case 'C':
                hash |= 0b01;
                break;
            case 'T':
                hash |= 0b10;
                break;
            case 'G':
                hash |= 0b11;
                break;
            default:
                hash |= 0b11;
                break;
        }

        switch (data.second[1][0]) {
            case 'A':
                hash |= 0b0000;
                break;
            case 'C':
                hash |= 0b0100;
                break;
            case 'T':
                hash |= 0b1000;
                break;
            case 'G':
                hash |= 0b1100;
                break;
            default:
                hash |= 0b1100;
                break;
        }

        return hash;
    }

    uint64_t _required_table_width() final { return 16; }
};

class TriplePrefixHasher : public PrefixHasher {
public:
    uint64_t _hash_fn(const std::pair<uint64_t, Read> &data) final {
        uint64_t hash = 0b0;
        switch (data.second[1][0]) {
            case 'A':
            case 'C':
                hash |= 0b00;
                break;
            case 'T':
            case 'G':
            default:
                hash |= 0b11;
                break;
        }

        switch (data.second[1][1]) {
            case 'A':
                hash |= 0b0000;
                break;
            case 'C':
                hash |= 0b0100;
                break;
            case 'T':
                hash |= 0b1000;
                break;
            case 'G':
                hash |= 0b1100;
                break;
            default:
                hash |= 0b1100;
                break;
        }

        switch (data.second[1][2]) {
            case 'A':
                hash |= 0b000000;
                break;
            case 'C':
                hash |= 0b010000;
                break;
            case 'T':
                hash |= 0b100000;
                break;
            case 'G':
                hash |= 0b110000;
                break;
            default:
                hash |= 0b110000;
                break;
        }

        return hash;
    }

    uint64_t _required_table_width() final { return 64; }
};

/*
 * PROCESSORS
 */

class FASTQProcessor : public DataProcessor<Read, std::string, std::string> {
    /*
     * Key Extraction Functions
     */
    std::string _extract_key_fn(Read &data) final {
        return data[1];
    }

    /*
     * Postprocessing functions
     */
    std::string _postprocess_fn(Read &data, std::string &value) override {
        return value;
    }
};


class RetaggingProcessor : public FASTQProcessor {
    /*
     * Postprocessing functions
     */
    std::string _postprocess_fn(Read &data, std::string &value) final {
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
};

/*
 *  PARSERS
 */

class FASTQParser : public DataParser<Read> {

    Read _parsing_fn (const std::shared_ptr<std::istream> &fin) override {
        //TODO fix error of SIGSEGV when reading from pipe (move operator problem?)
        std::string line;
        std::vector<std::string> lines(4);

        for (int i = 0; i < 4; i++) {
            std::getline(*fin, line);
            lines[i] = line;
        }

        return lines;
    }
};

class FASTAParser : public DataParser<Read> {

    Read _parsing_fn (const std::shared_ptr<std::istream> &fin) override {
        //TODO fix error of SIGSEGV when reading from pipe (move operator problem?)
        std::string line;
        std::vector<std::string> lines(4);

        for (int i = 0; i < 3; i++) {
            std::getline(*fin, line);
            lines[i] = line;
        }

        return lines;
    }
};

// Configure the data pipeline appropriately according to the config file
// T-dataType, K-cacheKey, V-cacheValue
void prep_experiment(ConfigParser &cfp, BucketedPipelineManager<Read, std::string, std::string> *pipe) {
    std::shared_ptr<OrderedSequenceStorage<std::pair<uint64_t, Read> > > bb;
    std::shared_ptr<InterleavedIOScheduler<Read>> io;
    io = std::make_shared<InterleavedIOScheduler<Read>>();

    /*
     * Cache Parameters
     */

    std::shared_ptr<CacheIndex<std::string, std::string> > c;
    c = std::make_shared<DummyCache<std::string, std::string> >();
    if (cfp.contains("cache_policy")) {
        std::string cache = cfp.get_val("cache_policy");
        if (cache == "lru") {
            c = std::make_shared<LRUCache<std::string, std::string> >();
        } else if (cache == "mru") {
            c = std::make_shared<MRUCache<std::string, std::string> >();
        }

        if (cfp.contains("cache_decorator")){
            std::string dec = cfp.get_val("cache_decorator");
            std::shared_ptr<CacheDecorator<std::string, std::string> > w;

            if (dec == "bloom_filter"){
                w = std::make_shared<BFECache<std::string, std::string> >();
                w->set_cache(c);
            }

            c = w;
        }
    }
    pipe->set_cache_subsystem(c);
    pipe->register_observer(c);

    /*
     * Storage Parameters
     */
    std::shared_ptr<DataHasher< std::pair<uint64_t, Read> > > h;
    if (cfp.contains("max_chain") && cfp.get_long_val("max_chain") == 1) {
        bb = std::make_shared<BufferedSortedChain<std::pair<uint64_t, Read> > >();
        ReadOdering order;
        bb->set_ordering(order);
    } else {
        bb = std::make_shared<BufferedBuckets<std::pair<uint64_t, Read> > >();
    }

    if (cfp.contains("num_buckets"))
        bb->set_num_buckets(cfp.get_long_val("num_buckets"));

    if (cfp.contains("bucket_size"))
        bb->set_bucket_size(cfp.get_long_val("bucket_size"));

    if (cfp.contains("hash_func")) {
        std::string hash_func = cfp.get_val("hash_func");
        if (hash_func == "double") {
            h = std::make_shared<DoublePrefixHasher>();
        } else if (hash_func == "triple") {
            h = std::make_shared<TriplePrefixHasher>();
        } else {
            // default is single prefix, also specified by "single"
            h = std::make_shared<PrefixHasher>();
        }
    } else {
        h = std::make_shared<NOPHasher>();
    }

    bb->set_data_properties(h);

    if (cfp.get_val("chain_switch") == "random") {
        bb->set_chain_switch_mode(ChainSwitch::RANDOM);
    }

    bb->register_observer(c);

    io->set_storage_subsystem(bb);

    /*
     * I/O Parameters
     */

    if (cfp.contains("async_io"))
        io->set_async_flag(cfp.get_bool_val("async_io"));

    if (cfp.contains("max_interleave"))
        io->set_max_interleave(cfp.get_long_val("max_interleave"));

    if (cfp.contains("input_pattern"))
        io->set_input_pattern(cfp.get_val("input_pattern"));

    if (cfp.contains("output_ext"))
        io->set_out_file_ext(cfp.get_val("output_ext"));

    if (cfp.contains("data_dir")) {
        try {
            io->from_dir(cfp.get_val("data_dir"));
        } catch (IOAssumptionFailedException &e) {
            std::cout << "No files to align. Stopping." << std::endl;
            exit(0);
        }
    }
    else if (cfp.get_bool_val("stdin"))
    {
        std::string out = "OUT.sam";
        io->from_stdin(out);
    }

    std::shared_ptr< DataParser<Read> > dr;
    dr = std::make_shared<FASTQParser>();
    if (cfp.contains("file_type")){
        if (cfp.get_val("file_type") == "fasta"){
            dr = std::make_shared<FASTAParser>();
        }
    }
    io->set_parser(dr);

    pipe->set_io_subsystem(io);

    /*
     * Pipeline Parameters
     */

    if (cfp.contains("compression")) {
        std::string cmp = cfp.get_val("compression");
        if (cmp == "full") {
            pipe->set_compression_level(FULL);
        } else if (cmp == "cross") {
            pipe->set_compression_level(CROSS);
        } else {
            pipe->set_compression_level(NONE);
        }
    }

    std::shared_ptr< DataProcessor<Read, std::string, std::string> > r;
    r = std::make_shared<FASTQProcessor>();
    if (cfp.contains("post_process_func")) {
        std::string post_proc = cfp.get_val("post_process_func");
        if (post_proc == "retag") {
            r = std::make_shared<RetaggingProcessor>();
        }
    }
    pipe->set_processor(r);

}

#endif //SEALM_PREP_EXPERIMENT_HPP
