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

void WrappedMapper::initialize_alignment() {
    if (_pipe.empty()){
        std::cout << "Nothing to align. Stopping." << std::endl;
        exit(0);
    } else {
        std::cout << "Files found matching input pattern:" << std::endl;
        for (const auto &f : _pipe.get_input_filenames()) {
            std::cout << f << std::endl;
        }
    }

    // TODO move this functionality to io system
    // check each file is readable
    for (const auto &in_file : _input_files) {
        std::ofstream fin(in_file);
        if (!fin){
            fin.close();
            std::cout << in_file << " not readable. Aborting." << std::endl;
            exit(1);
        }
        fin.close();
    }

    // clear or create output files
    for (const auto &out_file : _output_files) {
        std::ofstream fout(out_file);
        fout << "";
        fout.close();
    }

    // reset metrics
    _reads_aligned = 0;
    _align_time = 0;
    _reads_seen = 0;
    _total_time = 0;
}

WrappedMapper::WrappedMapper(CLIOptions &opts) {
    // extract necessary parameters
    _pipe.set_input_pattern(opts.input_file_pattern);
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
    if (_cache_type > 0) {
        std::stringstream command_s;
        command_s << "bowtie2 --mm --no-hd -p 3 -";
        command_s << _input_type;
        command_s << " -x ";
        command_s << _reference;
        command_s << " -U -";
        _command = command_s.str();
    } else {
        std::stringstream command_s;
        command_s << "bowtie2 --reorder --mm --no-hd -p 3 -";
        command_s << _input_type;
        command_s << " -x ";
        command_s << _reference;
        command_s << " -U -";
        _command = command_s.str();
    }

    // metrics
    _total_time = 0;
    _align_time = 0;
    _reads_seen = 0;
    _reads_aligned = 0;
    _align_calls = 0;
}

void WrappedMapper::run_alignment() {
    long start = std::chrono::duration_cast<Mills>(std::chrono::system_clock::now().time_since_epoch()).count();
    long align_start = 0, align_end = 0;

    initialize_alignment();

    _pipe.open();

    // call aligner to load reference into memory
    load_reference(_command);

    std::vector<std::string> alignments (_bucket_size);
    try{
        while(true){
            align_start = std::chrono::duration_cast<Mills>(
                    std::chrono::system_clock::now().time_since_epoch()).count();
            _pipe.next_bucket();

            _reads_seen += _pipe.current_bucket_size();
            align(_command, _pipe.get_unique_batch(), &alignments);

            _pipe.write(alignments);

            align_end = std::chrono::duration_cast<Mills>(std::chrono::system_clock::now().time_since_epoch()).count();

            // update state
            _align_calls++;
            _reads_aligned += alignments.size();
            _reads_seen += _pipe.current_bucket_size();
            _align_time += (align_end - align_start) / 1000.00;

            _throughput_vec.emplace_back(_bucket_size / ((align_end - align_start) / 1000.00));
            _hits_vec.emplace_back(_pipe->get_hits());
            _batch_time_vec.emplace_back(((align_end - align_start) / 1000.00));
            _reads_aligned_vec.emplace_back(_reads_aligned);

            // print metrics
            std::cout << "Batch Align Time: " << ((align_end - align_start) / 1000.00) << "s\n";
            std::cout << "Reads aligned " << _reads_aligned << "\n";
            std::cout << "Total reads " << _reads_seen << "\n";
            std::cout << _pipe;
            std::cout << "Throughput: " << (_bucket_size / ((align_end - align_start) / 1000.00)) << " r/s\n";
            std::cout << "Avg Throughput: " << (_reads_seen / _align_time) << " r/s\n";
            std::cout << "----------------------------" << std::endl;
        }
    } catch (IOResourceExhaustedException &ioree){
        // align final bucket
        align_start = std::chrono::duration_cast<Mills>(
                std::chrono::system_clock::now().time_since_epoch()).count();
        align(_command, _pipe.get_unique_batch(), &alignments);
        _pipe.write(alignments);
        align_end = std::chrono::duration_cast<Mills>(std::chrono::system_clock::now().time_since_epoch()).count();

        // update state
        _align_calls++;
        _reads_aligned += alignments.size();
        _reads_seen += _pipe.current_bucket_size();
        _align_time += (align_end - align_start) / 1000.00;

        _throughput_vec.emplace_back(_bucket_size / ((align_end - align_start) / 1000.00));
        _hits_vec.emplace_back(_pipe->get_hits());
        _batch_time_vec.emplace_back(((align_end - align_start) / 1000.00));
        _reads_aligned_vec.emplace_back(_reads_aligned);
    }

    // stop reading (in case of exception above) and flush output buffer
    _pipe.close();

    long end = std::chrono::duration_cast<Mills>(std::chrono::system_clock::now().time_since_epoch()).count();
    _total_time = (end - start) / 1000.00;
    std::cout << "===== COMPLETE =====" << std::endl;
}

std::string WrappedMapper::prepare_log() {
    std::stringstream ss;
    ss << "# batch_size:" <<_bucket_size << " manager_type:" << _manager_type << " cache_type:"
        << _cache_type << " total_reads:" << _reads_seen << " runtime:" << _total_time << std::endl;
    ss << "Batch,Batch_Time,Throughput,Hits,Reads_Aligned" << std::endl;
    for (uint64_t i = 0; i < _align_calls; i++) {
        ss << i << "," << _batch_time_vec[i] << "," << _throughput_vec[i] << "," << _hits_vec[i] << ","
           << _reads_aligned_vec[i] << std::endl;
    }

    return ss.str();
}