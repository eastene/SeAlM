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

// T-dataType of bucketed values
template<typename T>
class InterleavedIOScheduler {
private:
    // IO handles
    std::vector<std::pair<uint64_t, std::string> > _inputs; // pair of unique file id and file path
    std::vector<uint64_t> _seek_poses;
    std::vector<std::string> _outputs;
    uint64_t _read_head;

    // IO buffers
    BufferedBuckets<std::pair<uint64_t, T> > _storage_subsystem; // input storage
    std::vector<std::pair<uint64_t, std::string>> _out_buff; // simple outout buffer storage

    // Effort limits
    uint64_t _max_io_interleave;
    uint64_t _out_buff_threshold;
    std::chrono::milliseconds _max_wait_time;

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

    std::vector<T> parse_multi(uint64_t n); // reads multiple data points from a single file

    void read_until_done();

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

    std::unique_ptr<std::vector<std::pair<uint64_t, T> > > request_bucket();

    std::future<std::vector<std::pair<uint64_t, T> > > request_bucket_async();

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

    /*
     * State descriptors
     */

    bool empty() { return (_storage_subsystem.size() == 0) && (_inputs.empty()); }

    uint64_t size() { return _inputs.size(); }

};

/*
 * Functors
 */

template<typename T>
T default_parser(std::ifstream &fin) {
    std::string line;
    std::vector<std::string> lines(4);
    for (int i = 0; i < 4; i++) {
        std::getline(fin, line);
        lines[i] = line;
    }

    return lines;
}

/*
 * Method Implementations
 */

template<typename T>
InterleavedIOScheduler<T>::InterleavedIOScheduler() {
    _max_io_interleave = 10;
    _max_wait_time = std::chrono::milliseconds(5000);
    _read_head = 0;
    _halt_flag = false;
    _input_pattern = "";
    _auto_output_ext = "";
    _out_buff_threshold = 100000;
    _parsing_fn = std::function<T(std::ifstream &)>([](std::ifstream &fin) { return default_parser<T>(fin); });
}

template<typename T>
InterleavedIOScheduler<T>::InterleavedIOScheduler(const std::string &input_pattern,
                                                  std::function<T(std::ifstream)> &parse_func) {
    _max_io_interleave = 10;
    _max_wait_time = std::chrono::milliseconds(5000);
    _read_head = 0;
    _halt_flag = false;
    _input_pattern = input_pattern;
    _auto_output_ext = "_out";
    _out_buff_threshold = 100000;
    _parsing_fn = parse_func;
}

template<typename T>
void InterleavedIOScheduler<T>::from_dir(const std::experimental::filesystem::path &dir) {
    if (!std::experimental::filesystem::is_directory(dir)) {
        std::cout << "Cannot match any files for IO. Expected directory." << std::endl;
        return;
    }

    uint64_t i = 0;
    for (const auto &fs_obj : std::experimental::filesystem::directory_iterator(dir)) {
        if (regex_match(fs_obj.path().filename().string(), std::regex(_input_pattern))) {
            auto obj_path_mut = fs_obj.path();
            _inputs.emplace_back(std::make_pair(i++, obj_path_mut.string()));
            _outputs.emplace_back(obj_path_mut.replace_extension(_auto_output_ext));
            // TODO: make sure every file extension matches
            _input_ext = fs_obj.path().extension();
            _seek_poses.emplace_back(0);
        }
    }
}

template<typename T>
T InterleavedIOScheduler<T>::parse_single() {
    std::ifstream fin(_inputs[_read_head].second);
    fin.seekg(_seek_poses[_read_head]);

    T data = _parsing_fn(fin);

    if (fin.eof()) {
        throw IOResourceExhaustedException();
    }

    _seek_poses[_read_head] = fin.tellg();
    fin.close();

    return data;
}

template<typename T>
std::vector<T> InterleavedIOScheduler<T>::parse_multi(uint64_t n) {
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

template<typename T>
void InterleavedIOScheduler<T>::read_until_done() {
    while (!_halt_flag) {
        // parse data point(s) in round robin fashion until all files exhausted
        try {
            // TODO add timeout to reading
            _storage_subsystem.insert(std::make_pair(_inputs[_read_head].first, parse_single()));
        } catch (IOResourceExhaustedException &iosee) {
            // remove files after fully read
            _inputs.erase(_inputs.begin() + _read_head);
        }
        // move virtual read head to next file
        _read_head = (_inputs.size() == 0) ? 0 : (_read_head + 1) % std::min(_max_io_interleave, _inputs.size());

        // once all files have been read, flush any data remaining in buffers to buckets
        if (_inputs.empty()) {
            _storage_subsystem.flush();
            _halt_flag = true;
            //throw IOResourceExhaustedException();
        }
    }
}

template<typename T>
void InterleavedIOScheduler<T>::write_buffer(std::vector<std::pair<uint64_t, std::string>> _multiplexed_buff) {
    // put buffer in order of files to make writing more efficient
    std::sort(_multiplexed_buff.begin(), _multiplexed_buff.end(),
              [](const std::pair<uint64_t, std::string> &a, const std::pair<uint64_t, std::string> &b) {
                  return a.first < b.first;
              });

    // open first file
    uint64_t curr_file = _multiplexed_buff[0].first;
    std::ofstream fout(_outputs[curr_file]);

    // write each line in buffer, switching files when necessary
    for (const auto &mtpx_line : _multiplexed_buff) {
        if (mtpx_line.first != curr_file) {
            curr_file = mtpx_line.first;
            fout.close();
            fout.open(_outputs[curr_file]);
        }
        fout << mtpx_line.second;
    }
}

template<typename T>
bool InterleavedIOScheduler<T>::begin_reading() {
    // spawn reading daemon
    std::thread ([&]() { this->read_until_done(); }).detach();
    return true;
}

template<typename T>
bool InterleavedIOScheduler<T>::stop_reading() {
    // stop reading daemon
    _halt_flag = true;
    return true;
}

template<typename T>
std::unique_ptr<std::vector<std::pair<uint64_t, T> > > InterleavedIOScheduler<T>::request_bucket() {
    // request a bucket with deadlock detection
    auto future_bucket = std::move(_storage_subsystem.next_bucket_async());
    std::future_status status;
    status = future_bucket.wait_for(_max_wait_time);

    if (status == std::future_status::timeout) {
        // separate out this status for future event handling of timed out future
        _storage_subsystem.kill();
        return nullptr;
    } else if (status == std::future_status::ready) {
        return std::move(future_bucket.get());
    } else {
        _storage_subsystem.kill();
        // deferred, also separated out for future event handling of deferred future
        return nullptr;
    }
}

template<typename T>
std::future<std::vector<std::pair<uint64_t, T> > > InterleavedIOScheduler<T>::request_bucket_async() {
    return std::move(_storage_subsystem.next_bucket_async());
}

template<typename T>
void InterleavedIOScheduler<T>::write_async(uint64_t out_ind, std::string &line) {
    _out_buff.push_back(std::make_pair(out_ind, line));
    if (_out_buff.size() >= _out_buff_threshold) {
        std::thread([&](){write_buffer(_out_buff);}).detach();
    }
}

template<typename T>
void InterleavedIOScheduler<T>::flush() {
    write_buffer(_out_buff);
}

#endif //ALIGNER_CACHE_IO_HPP
