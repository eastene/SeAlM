//
// Created by evan on 4/19/19.
//

#include <thread>
#include <tuple>
#include <iostream>
#include <experimental/filesystem>
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

    _suppress_sam = opts.sam_suppress_flag;

    // command
    std::stringstream command_s;
    if (opts.aligner == "seal") {
        command_s << "seal threads=";
        command_s << opts.threads;
        command_s << " out=stdout.fq";
        command_s << " ref=";
        command_s << _reference;
        if (opts.interleaved)
            command_s << " interleaved=t";
        command_s << " in=stdin.fq";
        _command = command_s.str();
    } else {
        command_s << "bowtie2 --mm --no-hd -p ";
        command_s << opts.threads;
        command_s << " -";
        command_s << _input_type;
        command_s << " -x ";
        command_s << _reference;
        if (opts.interleaved)
            command_s << " --interleaved -";
        else
            command_s << " -U -";
        _command = command_s.str();
    }
}

WrappedMapper::WrappedMapper(ConfigParser &configs) {

    if (!configs.contains("reference")) {
        std::cout << "No reference file specified. Aborting." << std::endl;
        exit(1);
    }
    _reference = configs.get_val("reference");
    _bucket_size = configs.get_long_val("bucket_size");

    //std::set<std::string> formats {'.fastq', '.fasta', '.fa', '.fq'};
    //assert (['.fastq', '.fasta', '.fa', '.fq'])'
    //assert (os.path.splitext(self._output_file)[1].lower() == '.sam')

    prep_experiment(configs, &_pipe);

    if (configs.contains("metrics"))
        _metric_file = configs.get_val("metrics");
    else
        _metric_file = "";

    _config_file = configs.get_val("_this_config");

    // extract extra parameters
    _qual_thresh = 5225;

    // derived parameters
    _input_type = 'q'; // if input_format in ['.fasta', '.fa'] else 'q'
    _read_size = _input_type == 'q' ? 4 : 3;

    // aligner process command
    std::stringstream command_s;
    // allow user to specify custom command
    if (configs.contains("command")) {
        // TODO: get commands with spaces to work (better config parsing)
        command_s << configs.get_val("command");
    } else {
        // supported commands for certain aligners
        if (configs.get_val("aligner") == "seal") {
            if (configs.contains("aligner_path")) {
                command_s << configs.get_val("aligner_path");
            } else {
                command_s << "seal.sh";
            }
            command_s << " threads=";
            command_s << configs.get_val("threads");
            command_s << " out=stdout.fq";
            command_s << " ref=";
            command_s << _reference;
            if (configs.get_bool_val("interleaved"))
                command_s << " interleaved=t";
            command_s << " in=stdin.fq";
            command_s << " prealloc=t"; // similar to mm for bowtie2 ?
            // TODO: parameterize these memory-related values to seal
            command_s << " rskip=3";
            command_s << " -Xmx80G";
        } else {
            if (configs.contains("aligner_path")) {
                command_s << configs.get_val("aligner_path");
            } else {
                command_s << "bowtie2";
            }
            command_s << " --mm --no-hd -p ";
            if (configs.contains("threads"))
                command_s << configs.get_val("threads");
            else
                command_s << "1";
            command_s << " -";
            command_s << _input_type;
            command_s << " -x ";
            command_s << _reference;
            if (configs.get_bool_val("interleaved"))
                command_s << " --interleaved -";
            else
                command_s << " -U -";
        }
    }
    _command = command_s.str();
}

void WrappedMapper::run_alignment() {
    std::cout << "===== BEGINNING ALIGNMENT =====" << std::endl;

    double elapsed_time = 0.0;
    long this_bucket = 0;

    std::vector<std::string> alignments(_bucket_size);
    initialize_alignment();

    std::ofstream mfile;
    if (!_metric_file.empty()) {
        mfile.open(_metric_file);
        mfile << "# Num_Files:" << _pipe.get_filenames().size() << std::endl;
        mfile << "# File_Names:";
        for (const auto &infile : _pipe.get_filenames()) {
            mfile << infile << ",";
        }
        mfile << std::endl;
        mfile << "Batch,Batch_Time,Throughput,Hits,Misses,Reads_Aligned,Compression_Ratio" << std::endl;
    }

    _pipe.open();

    // align first bucket synchronously since aligner may have to load reference
    // and this data point should be ignored due to variable time spent loading
    auto read_future = _pipe.read_async();
    auto next_bucket = read_future.get();
    elapsed_time = call_aligner(_command, next_bucket, &alignments); // call once without timing to load reference
    std::cout << "Reference Load Time: " << elapsed_time << "s\n";

    long start = std::chrono::duration_cast<Mills>(std::chrono::system_clock::now().time_since_epoch()).count();
    try {
        while (true) {
            long batch_start = std::chrono::duration_cast<Mills>(
                    std::chrono::system_clock::now().time_since_epoch()).count();

            this_bucket = next_bucket.size();
            _reads_seen += _pipe.current_bucket_size();

            read_future = _pipe.read_async();

            //alignments.resize(next_bucket.size());

            elapsed_time = call_aligner(_command, next_bucket, &alignments);

            next_bucket = read_future.get();

            auto write_future = _pipe.write_async(alignments);

            long batch_end = std::chrono::duration_cast<Mills>(
                    std::chrono::system_clock::now().time_since_epoch()).count();

            // write_future.wait();
            // update state
            _align_calls++;
            _reads_aligned += this_bucket;
            _align_time += elapsed_time;
            _process_time += (batch_end - batch_start) / 1000.00;

            if (mfile) {
                mfile << _align_calls << "," << elapsed_time << "," << (_bucket_size / elapsed_time) << ","
                      << _pipe.cache_hits() << "," << _pipe.cache_misses() << ","
                      << _reads_aligned << "," << _pipe.current_compression_ratio() << std::endl;
                mfile.flush();
            } else {
                _throughput_vec.emplace_back(_bucket_size / elapsed_time);
                _hits_vec.emplace_back(_pipe.cache_hits());
                _batch_time_vec.emplace_back(elapsed_time);
                _reads_aligned_vec.emplace_back(_reads_aligned);
                _compression_ratio_vec.emplace_back(_pipe.current_compression_ratio());
            }

            // print metrics
            std::cout << "Batch Total Time: " << ((batch_end - batch_start) / 1000.00) << "s\n";
            std::cout << "  Batch Align Time: " << elapsed_time << "s\n";
            std::cout << "Total reads " << _reads_seen << "\n";
            std::cout << "  Reads aligned " << _reads_aligned << "\n";
            std::cout << "  Reads aligned this batch: " << this_bucket << "\n";
            std::cout << _pipe;
            std::cout << "Avg Throughput: " << (_reads_seen / _align_time) << " r/s\n";
            std::cout << "  Instant Throughput: " << (_bucket_size / elapsed_time) << " r/s\n";
            std::cout << "----------------------------" << std::endl;

            write_future.wait();
        }
    } catch (RequestToEmptyStorageException &rtese) {
        // update state
        _align_calls++;
        _reads_aligned += alignments.size();

        long batch_start = std::chrono::duration_cast<Mills>(
                std::chrono::system_clock::now().time_since_epoch()).count();

        auto write_future = _pipe.write_async(alignments);
        write_future.wait();

        long batch_end = std::chrono::duration_cast<Mills>(std::chrono::system_clock::now().time_since_epoch()).count();

        _process_time += (batch_end - batch_start) / 1000.00;
        if (mfile)
            mfile.close();
    }

    // stop reading (in case of exception above) and flush output buffer
    _pipe.close();

    long end = std::chrono::duration_cast<Mills>(std::chrono::system_clock::now().time_since_epoch()).count();
    _total_time = (end - start) / 1000.00;

    if (!_metric_file.empty()) {
        mfile.open(_metric_file, std::ios::app);
        mfile << "# config:" << _config_file << " total_reads:" << _reads_seen << " runtime:" << _total_time
              << std::endl;
    }

    std::cout << "===== COMPLETE =====" << std::endl;
}

std::string WrappedMapper::prepare_log() {
    std::stringstream ss;
    ss << "# batch_size:" << _bucket_size << " manager_type:" << _manager_type << " cache_type:"
       << _cache_type << " total_reads:" << _reads_seen << " runtime:" << _total_time << std::endl;
    ss << "Batch,Batch_Time,Throughput,Hits,Reads_Aligned,Compression_Ratio" << std::endl;
    for (uint64_t i = 0; i < _align_calls; i++) {
        ss << i << "," << _batch_time_vec[i] << "," << _throughput_vec[i] << "," << _hits_vec[i] << ","
           << _reads_aligned_vec[i] << "," << _compression_ratio_vec[i] << std::endl;
    }

    return ss.str();
}