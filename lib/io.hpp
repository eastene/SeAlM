//
// Created by Evan Stene on 2019-05-02.
//

#ifndef ALIGNER_CACHE_IO_HPP
#define ALIGNER_CACHE_IO_HPP

#include <regex>
#include <string>
#include <vector>
#include <cstdint>
#include <fstream>
#include <experimental/filesystem>

#include "storage.hpp"

/*
 * IO EXCEPTIONS
 */

class IOResourceExhaustedException : public std::exception {
};

class TimeoutException : public std::exception {

};

/*
 *
 * INTERLEAVED_IO_SCHEDULER
 *
 * Reads data from multiple files concurrently. Input is parsed using
 * a replaceable parsing function that returns a single data point of the
 * desired type and buffers it using buffered bucket storage. Handles requests
 * asynchronously for a single bucket that is managed by the desired batch
 * manager.
 *
 */

// T-dataType, M-Manager
template<typename T, typename M>
class InterleavedIOScheduler {
private:
    // IO handles
    std::vector<std::string> _inputs;
    std::vector<uint64_t> _seek_poses;
    std::vector<std::string> _outputs;
    uint64_t _read_head;

    // IO buffers
    BufferedBuckets<T> _in_buff;
    std::vector<std::pair<uint64_t, std::string>> _out_buff;

    // Effort limits
    uint16_t _max_io_interleave;
    uint64_t _out_buff_threshold;

    // Flags
    std::atomic_bool _halt_flag;

    // Automated file formatting variables
    std::string _input_pattern;
    std::string _input_ext;
    std::string _auto_output_ext; // used if modifying extension of input to generate output

    // Functors
    std::function<T(std::ifstream &)> _parsing_fn;

    // Private methods
    T parse_single(); // reads a single data point from a single file

    std::vector<T> parse_multi(uint64_t n);

    void read_until_done(std::atomic_bool &cancel);

    void write_buffer(std::vector<std::pair<uint64_t, std::string>> _multiplexed_buff);

public:
    InterleavedIOScheduler();

    InterleavedIOScheduler(const std::string &input_pattern, std::function<T(std::ifstream)> &parse_func);

    /*
     *  Convenience functions
     */
    void from_dir(const std::experimental::filesystem::path &dir);

    /*
     * Input functions
     */

    bool begin_reading();

    bool stop_reading();

    std::vector<T> request_bucket();

    /*
     * Output functions
     */

    // output must be string
    void write_async(uint64_t out_ind, std::string &line);

    void flush();

    /*
     * Getters/Setters
     */

    std::vector<std::string> get_input_filenames() { return _inputs; }

    void set_input_pattern(const std::string &input_pattern) { _input_pattern = input_pattern; }

    void set_input_files(const std::vector<std::string> &input_files);

    /*
     * State descriptors
     */

    bool empty() { return _in_buff.size() == 0; }

    uint64_t size() { return _inputs.size(); }

};

/*
 * Functors
 */

template<typename T>
T default_parser(std::ifstream &fin) {
    std::string line;
    std::getline(fin, line);
    return line;
}

/*
 * Method Implementations
 */

template<typename T, typename M>
InterleavedIOScheduler<T, M>::InterleavedIOScheduler() {
    _max_io_interleave = 10;
    _read_head = 0;
    _halt_flag = false;
    _input_pattern = "";
    _auto_output_ext = "";
    _out_buff_threshold = 100000;
    _parsing_fn = std::function<T(std::ifstream)>([](std::ifstream fin) { return default_parser<T>(fin); });
}

template<typename T, typename M>
InterleavedIOScheduler<T, M>::InterleavedIOScheduler(const std::string &input_pattern,
                                                     std::function<T(std::ifstream)> &parse_func) {
    _max_io_interleave = 10;
    _read_head = 0;
    _halt_flag = false;
    _input_pattern = input_pattern;
    _auto_output_ext = "_out";
    _out_buff_threshold = 100000;
    _parsing_fn = parse_func;
}

template<typename T, typename M>
void InterleavedIOScheduler<T, M>::from_dir(const std::experimental::filesystem::path &dir) {
    if (!std::experimental::filesystem::is_directory(dir)) {
        std::cout << "Cannot match any files for IO. Expected directory." << std::endl;
        return;
    }

    std::vector<std::string> filenames;
    for (const auto &fs_obj : std::experimental::filesystem::directory_iterator(dir)) {
        if (regex_match(fs_obj.path().filename().string(), std::regex(_input_pattern))) {
            auto obj_path_mut = fs_obj.path();
            _inputs.push_back(obj_path_mut.string());
            _outputs.push_back(obj_path_mut.replace_extension(_auto_output_ext));
            // TODO: make sure every file extension matches
            _input_ext = fs_obj.path().extension();
            filenames.push_back(obj_path_mut.filename().string());
        }
    }
}

template<typename T, typename M>
T InterleavedIOScheduler<T, M>::parse_single() {
    std::ifstream fin(_inputs[_read_head]);
    fin.seekg(_seek_poses[_read_head]);

    T data = _parsing_fn(fin);

    if (fin.eof()) {
        throw IOResourceExhaustedException();
    }

    _seek_poses[_read_head] = fin.tellg();
    fin.close();

    return data;
}

template<typename T, typename M>
std::vector<T> InterleavedIOScheduler<T, M>::parse_multi(uint64_t n) {
    std::vector<T> data;
    std::ifstream fin(_inputs[_read_head]);
    fin.seekg(_seek_poses[_read_head]);

    for (uint64_t i = 0; i < n; i++) {
        data.push_back(_parsing_fn(fin));

        if (fin.eof()) {
            throw IOResourceExhaustedException();
        }
    }

    _seek_poses[_read_head] = fin.tellg();
    fin.close();

    return data;
}

template<typename T, typename M>
void InterleavedIOScheduler<T, M>::read_until_done(std::atomic_bool &cancel) {
    while (!cancel) {
        // parse data point(s) in round robin fashion until all files exhausted
        try {
            // TODO add timeout to reading
            _in_buff.insert(parse_single(), cancel);
        } catch (IOResourceExhaustedException &iosee) {
            // remove files after fully read
            _inputs.erase(_inputs.begin() + _read_head);
        }
        // move virtual read head to next file
        _read_head = (_read_head + 1) % std::min(_max_io_interleave, _inputs.size());

        // once all files have been read, flush any data remaining in buffers to buckets
        if (_inputs.empty()) {
            _in_buff.flush();
            cancel = true;
            throw IOResourceExhaustedException();
        }
    }
}

template<typename T, typename M>
void InterleavedIOScheduler<T, M>::write_buffer(std::vector<std::pair<uint64_t, std::string>> _multiplexed_buff) {
    // put buffer in order of files to make writing more efficient
    std::sort(_multiplexed_buff.begin(), _multiplexed_buff.end(),
              [](const std::pair<uint64_t, std::string> &a, const std::pair<uint64_t, std::string> &b) {
                  return a.first < b.first;
              });

    // open first file
    uint64_t curr_file = _multiplexed_buff[0].first;
    uint64_t i = 0;
    std::ofstream fout(_outputs[curr_file]);

    // write each line in buffer, switching files when necessary
    for (const auto &mtpx_line : _multiplexed_buff){
        if (_multiplexed_buff[i].first != curr_file){
            curr_file = _multiplexed_buff[i].first;
            fout.close();
            fout.open(_outputs[curr_file]);
        }
        fout.write(_multiplexed_buff[i].second);
    }
}

template<typename T, typename M>
bool InterleavedIOScheduler<T, M>::begin_reading() {
    std::thread(read_until_done, _halt_flag);
}

template<typename T, typename M>
bool InterleavedIOScheduler<T, M>::stop_reading() {
    _halt_flag = false;
}

template<typename T, typename M>
std::vector<T> InterleavedIOScheduler<T, M>::request_bucket() {
    std::unique_ptr<std::vector<T>> bucket = _in_buff.next_bucket_async(std::chrono::milliseconds(500));
    if (bucket == nullptr) {
        throw TimeoutException();
    }
    return bucket.get();
}

template<typename T, typename M>
void InterleavedIOScheduler<T, M>::write_async(uint64_t out_ind, std::string &line) {
    _out_buff.push_back(std::make_pair(out_ind, line));
    if (_out_buff >= _out_buff_threshold) {
        std::thread(write_buffer, _out_buff).detach();
    }
}

template<typename T, typename M>
void InterleavedIOScheduler<T, M>::flush() {
    write_buffer(_out_buff);
}

#endif //ALIGNER_CACHE_IO_HPP
