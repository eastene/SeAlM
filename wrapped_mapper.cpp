//
// Created by evan on 4/19/19.
//

#include <thread>
#include <tuple>
#include <iostream>
#include "wrapped_mapper.hpp"
#include "utils.hpp"

WrappedMapper::WrappedMapper() {
    // extract necessary parameters
    _input_file = "/home/evan/CLionProjects/aligner_cache/data/Sample_1_concat.fastq";
    _reference = "/home/evan/CLionProjects/aligner_cache/ref/bt2/GCA_000001405.15_GRCh38_no_alt_analysis_set.fna.bowtie_index";
    _output_file = "/home/evan/CLionProjects/aligner_cache/out.sam";
    _input_format = _input_file.substr(_input_file.rfind('.') + 1);
    //std::set<std::string> formats {'.fastq', '.fasta', '.fa', '.fq'};
    //assert (['.fastq', '.fasta', '.fa', '.fq'])'
    //assert (os.path.splitext(self._output_file)[1].lower() == '.sam')

    // extract extra parameters
    _batch_size = 100000;  // batch size of 100000 seems to work best for bt2 for effectiveness of batch-cache
    _qual_thresh = 5225;
    assert(_batch_size > 0);

    // derived parameters
    _input_type = 'q'; // if input_format in ['.fasta', '.fa'] else 'q'
    _read_size = _input_type == 'q' ? 4 : 3;

    // batch manager with cache
    _batch_manager = batch::CompressedBatchManager(_batch_size);

    // command
    std::stringstream command_s;
    command_s << "bowtie2 --reorder --mm --no-hd -p 3 -";
    command_s << _input_type;
    command_s << " -x ";
    command_s << _reference;
    command_s << " -U -";
    _command = command_s.str();

    // metrics
    _total_time = 0;
    _align_time = 0;
    _reads_seen = 0;
    _reads_aligned = 0;
    _align_calls = 0;
//        intthroughput_vec;
//        self._hits_vec = []
}

void WrappedMapper::run_alignment() {
    long start = std::chrono::duration_cast<Mills>(std::chrono::system_clock::now().time_since_epoch()).count();

    // clear output file
    std::ofstream fout(_output_file);
    fout << "";

    // reset metrics
    _reads_aligned = 0;
    _align_time = 0;
    _reads_seen = 0;
    _total_time = 0;

    // align batches of reads
    std::ifstream fin(_input_file);
    if (!fin) {
        std::cout << _input_file << " not found. Exiting." << std::endl;
        exit(1);
    }
    fin.close();

    // open process for output writing
    bool more_data = true;
    std::string temp;
    temp.reserve(150);
    std::vector<Read> in_buffer(_batch_size, Read(4, temp));
    std::vector<std::string> out;
    uint64_t seek_pos = 0;

    next_batch(_input_file, _batch_size, &in_buffer, &_batch_manager, &more_data, &seek_pos);
    std::vector<Read> prev_reduced_batch = _batch_manager.get_reduced_batch();
    std::vector<RedupeRef> prev_batch = _batch_manager.get_batch();

    long align_start=0, align_end=0;

    while (more_data) {
        align_start = std::chrono::duration_cast<Mills>(
                std::chrono::system_clock::now().time_since_epoch()).count();

//            qual_sum = sum(array.array('B', list(read[1])))
//            if qual_sum < self.qual_thresh:
//            if quality is too low, force aligner to try to handle it
//            batch.append(read)
//            continue

        // perform alignment on reduced batch
        std::thread th_b(next_batch, std::ref(_input_file), _batch_size, &in_buffer, &_batch_manager, &more_data,
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

        _batch_manager.cache_batch(prev_reduced_batch, out);
        prev_reduced_batch = _batch_manager.get_reduced_batch();
        prev_batch = _batch_manager.get_batch();

        // print metrics
        _align_calls++;
        _reads_aligned += out.size();
        align_end = std::chrono::duration_cast<Mills>(std::chrono::system_clock::now().time_since_epoch()).count();
        _align_time += (align_end - align_start) / 1000.00;
        std::cout << "Batch Align Time: " << ((align_end - align_start) / 1000.00) << "s\n";
        std::cout << "Reads aligned " << _reads_aligned << "\n";
        std::cout << "Total reads " << _reads_seen << "\n";
        std::cout << _batch_manager;
        std::cout << "Throughput: " << (_reads_seen / _align_time) << " r/s\n";
        _throughput_vec.emplace_back(_reads_seen / _align_time);
        _hits_vec.emplace_back(_batch_manager.get_hits());
    }

    if (!prev_batch.empty()) {
        align_start = std::chrono::duration_cast<Mills>(
                std::chrono::system_clock::now().time_since_epoch()).count();
        align(_command, prev_reduced_batch, &out);
        write_batch(_output_file, prev_batch, out);
        _align_calls++;
        _reads_aligned += out.size();
        align_end = std::chrono::duration_cast<Mills>(std::chrono::system_clock::now().time_since_epoch()).count();
        _align_time += (align_end - align_start) / 1000.00;
    }

    long end = std::chrono::duration_cast<Mills>(std::chrono::system_clock::now().time_since_epoch()).count();
    _total_time = (end - start) / 1000.00;
    std::cout << "===== COMPLETE =====" << std::endl;
}

std::string WrappedMapper::prepare_log() {
    std::stringstream ss;
    ss << "Batch,Throughput,Hits" << std::endl;
    for (uint64_t i = 0; i < _align_calls; i++) {
        ss << i << "," << _throughput_vec[i] << "," << _hits_vec[i] << std::endl;
    }

    return ss.str();
}