//
// Created by evan on 4/19/19.
//

#include <thread>
#include <tuple>
#include <iostream>
#include <experimental/filesystem>
#include <regex>
#include "wrapped_mapper.hpp"
#include "mapping_utils.hpp"
#include "prep_experiment.hpp"

void WrappedMapper::initialize_alignment() {
    if (_pipe.empty()) {
        std::cout << "Nothing to align. Stopping." << std::endl;
        exit(0);
    }
//    else {
//        std::cout << "Files found matching input pattern:" << std::endl;
//        for (const auto &f : _pipe.get_input_filenames()) {
//            std::cout << f << std::endl;
//        }
//    }

    // TODO move this functionality to io system
    // check each file is readable
//    for (const auto &in_file : _input_files) {
//        std::ofstream fin(in_file);
//        if (!fin) {
//            fin.close();
//            std::cout << in_file << " not readable. Aborting." << std::endl;
//            exit(1);
//        }
//        fin.close();
//    }
//
//    // clear or create output files
//    for (const auto &out_file : _output_files) {
//        std::ofstream fout(out_file);
//        fout << "";
//        fout.close();
//    }

    // reset metrics
    _reads_aligned = 0;
    _align_time = 0;
    _reads_seen = 0;
    _total_time = 0;
    _align_calls = 0;
}

WrappedMapper::WrappedMapper(CLIOptions &opts) {
    // extract necessary parameters
    _params.input_file_pattern = opts.input_file_pattern;
    _params.data_dir = opts.data_dir;
    _pipe.set_params(_params);
    _reference = opts.reference;
    //std::set<std::string> formats {'.fastq', '.fasta', '.fa', '.fq'};
    //assert (['.fastq', '.fasta', '.fa', '.fq'])'
    //assert (os.path.splitext(self._output_file)[1].lower() == '.sam')

    // extract extra parameters
    _bucket_size = opts.batch_size;  // batch size of 100000 seems to work best for bt2 for effectiveness of batch-cache
    _qual_thresh = 5225;
    _cache_type = opts.cache_type;
    _manager_type = opts.manager_type;
    assert(_bucket_size > 0);

    // derived parameters
    _input_type = 'q'; // if input_format in ['.fasta', '.fa'] else 'q'
    _read_size = _input_type == 'q' ? 4 : 3;

    // command
    std::stringstream command_s;
    command_s << "bowtie2 --mm --no-hd -p 3 -";
    command_s << _input_type;
    command_s << " -x ";
    command_s << _reference;
    command_s << " -U -";
    _command = command_s.str();

}

WrappedMapper::WrappedMapper(ConfigParser &configs) {

    if (!configs.contains("reference")) {
        std::cout << "No reference file specified. Aborting." << std::endl;
        exit(1);
    }
    _reference = configs.get_val("reference");

    //std::set<std::string> formats {'.fastq', '.fasta', '.fa', '.fq'};
    //assert (['.fastq', '.fasta', '.fa', '.fq'])'
    //assert (os.path.splitext(self._output_file)[1].lower() == '.sam')

    prep_experiment(configs, &_pipe);

    // extract extra parameters
    _qual_thresh = 5225;

    // derived parameters
    _input_type = 'q'; // if input_format in ['.fasta', '.fa'] else 'q'
    _read_size = _input_type == 'q' ? 4 : 3;

    // command
    // TODO: Allow command to come from config
    std::stringstream command_s;
    command_s << "bowtie2 --reorder --mm --no-hd -p 3 -";
    command_s << _input_type;
    command_s << " -x ";
    command_s << _reference;
    command_s << " -U -";
    _command = command_s.str();

}

void WrappedMapper::run_alignment() {
    long start = std::chrono::duration_cast<Mills>(std::chrono::system_clock::now().time_since_epoch()).count();
    double elapsed_time = 0.0;
    long this_bucket = 0;

    std::vector<std::string> alignments;
    initialize_alignment();

    _pipe.open();

    // align first bucket synchronously since aligner may have to load reference
    // and this data point should be ignored due to variable time spent loading
    auto read_future = _pipe.read_async();
    auto next_bucket = read_future.get();

    try {
        while (true) {
            this_bucket = _pipe.current_bucket_size();
            _reads_seen += this_bucket;
            read_future = _pipe.read_async();

            alignments.resize(next_bucket.size());

            elapsed_time = call_aligner(_command, next_bucket, &alignments);

            next_bucket = read_future.get();

            auto write_future = _pipe.write_async(alignments);

            // update state
            _align_calls++;
            _reads_aligned += alignments.size();
            _align_time += elapsed_time;

            _throughput_vec.emplace_back(this_bucket / elapsed_time);
            _hits_vec.emplace_back(_pipe.cache_hits());
            _batch_time_vec.emplace_back(elapsed_time);
            _reads_aligned_vec.emplace_back(_reads_aligned);

            // print metrics
            std::cout << "Batch Align Time: " << elapsed_time << "s\n";
            std::cout << "Reads aligned " << _reads_aligned << "\n";
            std::cout << "Total reads " << _reads_seen << "\n";
            std::cout << _pipe;
            std::cout << "Throughput: " << (this_bucket / elapsed_time) << " r/s\n";
            std::cout << "Avg Throughput: " << (_reads_seen / _align_time) << " r/s\n";
            std::cout << "----------------------------" << std::endl;
            write_future.wait();
        }
    } catch (RequestToEmptyStorageException &rtese) {
        _reads_aligned += alignments.size();
        auto write_future = _pipe.write_async(alignments);
        write_future.wait();
    }

    // stop reading (in case of exception above) and flush output buffer
    _pipe.close();

    long end = std::chrono::duration_cast<Mills>(std::chrono::system_clock::now().time_since_epoch()).count();
    _total_time = (end - start) / 1000.00;
    std::cout << "===== COMPLETE =====" <<
              std::endl;
}

std::string WrappedMapper::prepare_log() {
    std::stringstream ss;
    ss << "# batch_size:" << _bucket_size << " manager_type:" << _manager_type << " cache_type:"
       << _cache_type << " total_reads:" << _reads_seen << " runtime:" << _total_time << std::endl;
    ss << "Batch,Batch_Time,Throughput,Hits,Reads_Aligned" << std::endl;
    for (uint64_t i = 0; i < _align_calls; i++) {
        ss << i << "," << _batch_time_vec[i] << "," << _throughput_vec[i] << "," << _hits_vec[i] << ","
           << _reads_aligned_vec[i] << std::endl;
    }

    return ss.str();
}