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

class IOStreamExhaustedException : public std::exception {
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
    std::vector<std::string> _out_buff;

    // Effort limits
    uint16_t _max_io_interleave;

    // Flags
    std::atomic_bool _halt_flag;

    // Automated file formatting variables
    std::string _input_pattern;
    std::string _input_ext;
    std::string _auto_output_ext; // used if modifying extension of input to generate output

    // Private methods
    T parse_fn(); // reads a single data point from a single file

    std::vector<T> parse_multi(uint32_t n);

    void read_until_done(std::atomic_bool &cancel);

public:
    InterleavedIOScheduler();

    InterleavedIOScheduler(const std::string &input_pattern);

    /*
     *  Convenience functions
     */
    void from_dir(const std::experimental::filesystem::path &dir);

    /*
     * Input functions
     */

    bool begin_reading();

    bool stop_reading();

    std::vector<T> request_batch();

    /*
     * Output functions
     */

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

template<typename T, typename M>
InterleavedIOScheduler<T, M>::InterleavedIOScheduler() {
    _max_io_interleave = 10;
    _read_head = 0;
    _halt_flag = false;
    _input_pattern = "";
    _auto_output_ext = "";
}

template<typename T, typename M>
InterleavedIOScheduler<T, M>::InterleavedIOScheduler(const std::string &input_pattern) {
    _max_io_interleave = 10;
    _read_head = 0;
    _halt_flag = false;
    _input_pattern = input_pattern;
    _auto_output_ext = "";
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
T InterleavedIOScheduler<T, M>::parse_fn() {
    T data;
    std::ifstream fin(_inputs[_read_head]);
    fin.seekg(_seek_poses[_read_head]);

    std::string line;
    std::getline(fin, line);
    // TODO define parse function as lambda
    if (fin.eof()) {
        throw IOStreamExhaustedException();
    }

    _seek_poses[_read_head] = fin.tellg();
    fin.close();
}

template<typename T, typename M>
void InterleavedIOScheduler<T, M>::read_until_done(std::atomic_bool &cancel) {
    while (!cancel) {
        try {
            // TODO add timeout to reading
            _in_buff.insert(parse_fn(), cancel);
        } catch (IOStreamExhaustedException &iosee) {
            _inputs.erase(_inputs.begin() + _read_head);
        }
        _read_head = (_read_head + 1) % _max_io_interleave;
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
std::vector<T> InterleavedIOScheduler<T, M>::request_batch() {
    std::unique_ptr<std::vector<T>> bucket = _in_buff.next_bucket_async(std::chrono::milliseconds(500));
    if (bucket == nullptr) {
        throw TimeoutException();
    }
    return bucket.get();
}

#endif //ALIGNER_CACHE_IO_HPP
