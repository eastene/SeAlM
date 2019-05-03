//
// Created by evan on 4/19/19.
//

#include <thread>
#include <tuple>
#include <iostream>
#include <experimental/filesystem>
#include <regex>
#include "wrapped_mapper.hpp"
#include "utils.hpp"

void WrappedMapper::initialize_alignment() {
    if (io.empty()){
        std::cout << "Nothing to align. Stopping." << std::endl;
        exit(0);
    } else {
        std::cout << "Files found matching input pattern:" << std::endl;
        for (const auto &f : io.get_input_filenames()) {
            std::cout << f << std::endl;
        }
    }

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
    io.set_input_pattern(opts.input_file_pattern);
    _reference = opts.reference;
    //std::set<std::string> formats {'.fastq', '.fasta', '.fa', '.fq'};
    //assert (['.fastq', '.fasta', '.fa', '.fq'])'
    //assert (os.path.splitext(self._output_file)[1].lower() == '.sam')

    // extract extra parameters
    _batch_size = opts.batch_size;  // batch size of 100000 seems to work best for bt2 for effectiveness of batch-cache
    _qual_thresh = 5225;
    _cache_type = opts.cache_type;
    _manager_type = opts.manager_type;
    assert(_batch_size > 0);

    // derived parameters
    _input_type = 'q'; // if input_format in ['.fasta', '.fa'] else 'q'
    _read_size = _input_type == 'q' ? 4 : 3;

    // batch manager with cache
    switch (_manager_type) {
        case 0:
            std::cout << "Selecting default batch manager." << std::endl;
            _batch_manager = std::make_shared<BucketManager>(_batch_size, _cache_type);
            break;
        case 1:
            std::cout << "Selecting compressed batch manager." << std::endl;
            _batch_manager = std::make_shared<CompressedBucketManager>(_batch_size, _cache_type);
            break;
        default:
            _batch_manager = std::make_shared<BucketManager>(_batch_size, _cache_type);
    }

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

    initialize_alignment();

    io.begin_reading();

    // open process for output writing
    bool more_data = true;
    std::string temp;
    temp.reserve(150);
    std::vector<Read> in_buffer(_batch_size, Read(4, temp));
    std::vector<std::string> out;
    uint64_t seek_pos = 0;

    next_batch(_input_files, _batch_size, &in_buffer, _batch_manager, &more_data, &seek_pos);
    std::vector<Read> prev_reduced_batch = _batch_manager->get_unique_batch();
    std::vector<RedupeRef> prev_batch = _batch_manager->get_reduced_batch();

    long align_start = 0, align_end = 0;

    while (more_data) {
        align_start = std::chrono::duration_cast<Mills>(
                std::chrono::system_clock::now().time_since_epoch()).count();

//            qual_sum = sum(array.array('B', list(read[1])))
//            if qual_sum < self.qual_thresh:
//            if quality is too low, force aligner to try to handle it
//            batch.append(read)
//            continue

        // perform alignment on reduced batch
        std::thread th_b(next_batch, std::ref(_input_file), _batch_size, &in_buffer, _batch_manager, &more_data,
                         &seek_pos);
//        std::thread th_a(align, std::ref(_command), std::ref(prev_reduced_batch), &out);
        align(_command, prev_reduced_batch, &out);
        //      th_a.join();
        th_b.join();

        // TODO: make this equal to what th_b returns
        _reads_seen += _batch_size;
        //write_batch(_output_file, prev_batch, out);
        std::thread th_w(write_batch, std::ref(_output_file), prev_batch, out);
        th_w.detach();

        // TODO: implement lock for cache so this can be detached instead of joining
        std::thread th_c([=] { _batch_manager->cache_batch(prev_reduced_batch, out); });

        //_batch_manager->cache_batch(prev_reduced_batch, out);

        prev_reduced_batch = _batch_manager->get_unique_batch();
        prev_batch = _batch_manager->get_reduced_batch();

        // print metrics
        _align_calls++;
        _reads_aligned += out.size();
        align_end = std::chrono::duration_cast<Mills>(std::chrono::system_clock::now().time_since_epoch()).count();
        _align_time += (align_end - align_start) / 1000.00;
        std::cout << "Batch Align Time: " << ((align_end - align_start) / 1000.00) << "s\n";
        std::cout << "Reads aligned " << _reads_aligned << "\n";
        std::cout << "Total reads " << _reads_seen << "\n";
        std::cout << *_batch_manager;
        std::cout << "Throughput: " << (_batch_size / ((align_end - align_start) / 1000.00)) << " r/s\n";
        std::cout << "Avg Throughput: " << (_reads_seen / _align_time) << " r/s\n";
        std::cout << "----------------------------" << std::endl;
        _throughput_vec.emplace_back(_batch_size / ((align_end - align_start) / 1000.00));
        _hits_vec.emplace_back(_batch_manager->get_hits());
        _batch_time_vec.emplace_back(((align_end - align_start) / 1000.00));
        _reads_aligned_vec.emplace_back(_reads_aligned);

        th_c.join();
    }

    if (!prev_batch.empty()) {
        align_start = std::chrono::duration_cast<Mills>(
                std::chrono::system_clock::now().time_since_epoch()).count();
        align(_command, prev_reduced_batch, &out);
        write_batch(_output_file, prev_batch, out);
        _align_calls++;
        _reads_aligned += out.size();
        _reads_seen += prev_batch.size();
        align_end = std::chrono::duration_cast<Mills>(std::chrono::system_clock::now().time_since_epoch()).count();
        _align_time += (align_end - align_start) / 1000.00;

        _throughput_vec.emplace_back(prev_batch.size() / ((align_end - align_start) / 1000.00));
        _hits_vec.emplace_back(_batch_manager->get_hits());
        _batch_time_vec.emplace_back(((align_end - align_start) / 1000.00));
        _reads_aligned_vec.emplace_back(_reads_aligned);
    }

    long end = std::chrono::duration_cast<Mills>(std::chrono::system_clock::now().time_since_epoch()).count();
    _total_time = (end - start) / 1000.00;
    std::cout << "===== COMPLETE =====" << std::endl;
}

std::string WrappedMapper::prepare_log() {
    std::stringstream ss;
    ss << "# batch_size:" <<_batch_size << " manager_type:" << _manager_type << " cache_type:"
        << _cache_type << " total_reads:" << _reads_seen << " runtime:" << _total_time << std::endl;
    ss << "Batch,Batch_Time,Throughput,Hits,Reads_Aligned" << std::endl;
    for (uint64_t i = 0; i < _align_calls; i++) {
        ss << i << "," << _batch_time_vec[i] << "," << _throughput_vec[i] << "," << _hits_vec[i] << ","
           << _reads_aligned_vec[i] << std::endl;
    }

    return ss.str();
}