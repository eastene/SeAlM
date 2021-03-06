//
// Created by Evan Stene on 2019-05-02.
//

#ifndef SEALM_IO_HPP
#define SEALM_IO_HPP

#include <regex>
#include <string>
#include <vector>
#include <cstdint>
#include <fstream>
#include <experimental/filesystem>

#include "storage.hpp"
#include "logging.hpp"

namespace SeAlM {
/*
 * IO EXCEPTIONS
 */

// failure of any assumptions IO module requires (e.g. input files exist)
    class IOAssumptionFailedException : public std::exception {
    };

    class IOResourceExhaustedException : public std::exception {
    };

    class RequestToEmptyStorageException : public std::exception {
    };

    class TimeoutException : public std::exception {
    };

/*
 * DATA PARSER
 */

// T - sequence Type (string, vector, etc.)
template<typename T>
class DataParser {
public:
    virtual ~DataParser(){};
    // parse from an istream (can add more input sources later)
    virtual void _parsing_fn(const std::shared_ptr<std::istream> &in, T* out) = 0;
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
        std::vector<std::shared_ptr<std::istream> > _in_streams;
        std::vector<std::shared_ptr<std::ostream> > _out_streams;
        std::vector<uint64_t> _seek_poses;
        std::vector<std::string> _outputs;
        uint64_t _read_head;

        // IO buffers
        std::shared_ptr<OrderedSequenceStorage<std::pair<uint64_t, T> > > _storage_subsystem; // input storage
        std::vector<std::pair<uint64_t, PreHashedString>> _out_buff; // simple outout buffer storage

        // Effort limits
        uint64_t _max_io_interleave;
        uint64_t _out_buff_threshold;
        std::chrono::milliseconds _max_wait_time;

        // Flags
        bool _from_stdin; // can only read sequentially,
        std::atomic_bool _reading;
        std::atomic_bool _halt_flag;
        std::atomic_bool _storage_full_flag;
        std::atomic_bool _storage_empty_flag;
        std::atomic_bool _async_fill_flag;
        std::atomic_bool _suppress_output; // don't write outputs (emulates writing to /dev/null)

        // Automated file formatting variables
        std::string _input_pattern;
        std::string _input_ext;
        std::string _auto_output_ext; // used if modifying extension of input to generate output

        // Functors
        std::shared_ptr<DataParser<T> > _parser;

        // Private methods
        T parse_single(); // reads a single data point from a single file

        std::vector<T> parse_multi(uint64_t n); // reads multiple data points from a single file

        void read_until_done(); // asynchronous continuous fill

        void read_until_full(); // synchronous fill -> empty cycle

        void write_buffer(std::vector<std::pair<uint64_t, PreHashedString>> &_multiplexed_buff);

        void write_buffer_multiplexed(std::vector<std::pair<uint64_t, PreHashedString>> &_multiplexed_buff);

    public:
        InterleavedIOScheduler();

        ~InterleavedIOScheduler();

        /*
         *  Convenience functions
         */
        void from_dir(const std::experimental::filesystem::path &dir);

        void from_stdin(std::string &out);

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
        void write_async(uint64_t out_ind, PreHashedString &line);

        void flush();

        /*
         * Getters/Setters
         */

        std::vector<std::string> get_input_filenames();

        void set_async_flag(bool flag) { _async_fill_flag.store(flag); }

        void set_max_interleave(uint64_t max_interleave) { _max_io_interleave = max_interleave; }

        void set_input_pattern(const std::string &input_pattern) { _input_pattern = input_pattern; }

        void set_out_file_ext(const std::string &file_ext) { _auto_output_ext = file_ext; }

        void set_storage_subsystem(std::shared_ptr<OrderedSequenceStorage<std::pair<uint64_t, T> > > &other) {
            _storage_subsystem = other;
        }

        void set_parser(std::shared_ptr<DataParser<T> > &other) { _parser = other; }

        void suppress_output(bool suppress) { _suppress_output = suppress; }

        /*
         * State Descriptors
         */

        bool empty() { return (_storage_subsystem->size() == 0) && (_inputs.empty()); }

        bool full() { return _storage_subsystem->full(); }

        uint64_t size() { return _inputs.size(); }

        uint64_t capacity() { return _storage_subsystem->capacity(); }

        bool eoc() { return _storage_subsystem->eoc(); }

        /*
         * Operator Overloads
         */

        InterleavedIOScheduler &operator=(const InterleavedIOScheduler &&other);

        // don't allow copying (contains streams)
        InterleavedIOScheduler &operator=(const InterleavedIOScheduler &other) = delete;
    };

/*
 * Method Implementations
 */

    template<typename T>
    InterleavedIOScheduler<T>::InterleavedIOScheduler() {
        _max_io_interleave = 1;
        _max_wait_time = std::chrono::milliseconds(5000);
        _read_head = 0;
        _input_pattern = "";
        _auto_output_ext = "_out";
        _out_buff_threshold = 200000;
        _out_buff.reserve(_out_buff_threshold);

        _halt_flag.store(false);
        _async_fill_flag.store(true);
        _storage_full_flag.store(false);
        _storage_empty_flag.store(true);
        _reading.store(false);
        _suppress_output.store(false);

        log_debug("Default IO module initiated.");
    }

    template<typename T>
    InterleavedIOScheduler<T>::~InterleavedIOScheduler() {
        log_debug("Deleting IO module.");
        if (!_in_streams.empty() && dynamic_cast<std::ifstream *>(_in_streams[0].get()) != nullptr) {
            log_debug("Closing input streams.");
            for (const auto &strm : _in_streams) {
                dynamic_cast<std::ifstream *>(strm.get())->close();
            }
        }

        if (!_out_streams.empty() && dynamic_cast<std::ofstream *>(_out_streams[0].get()) != nullptr) {
            log_debug("Closing output streams.");
            for (const auto &strm : _out_streams) {
                dynamic_cast<std::ofstream *>(strm.get())->close();
            }
        }

        _in_streams.clear();
        _out_streams.clear();
    }

    template<typename T>
    void InterleavedIOScheduler<T>::from_dir(const std::experimental::filesystem::path &dir) {
        // throw error if data directory is not a directory
        if (!std::experimental::filesystem::is_directory(dir)) {
            log_error(dir.string() + " is not a directory or does not exist.");
            throw IOAssumptionFailedException();
        }

        log_info("Looking for files matching " + _input_pattern + " in " + dir.string());

        uint64_t i = 0;
        for (const auto &fs_obj : std::experimental::filesystem::directory_iterator(dir)) {
            if (regex_match(fs_obj.path().filename().string(), std::regex(_input_pattern))) {
                auto obj_path_mut = fs_obj.path();
                // Inputs
                _inputs.emplace_back(std::make_pair(i++, obj_path_mut.string()));

                // open new ifstream (necessary to avoid closing after going out of scope)
                _in_streams.emplace_back(new std::ifstream);
                dynamic_cast<std::ifstream *>(_in_streams[i - 1].get())->open(obj_path_mut);

                // Outputs
                if (!_suppress_output) {
                    _outputs.emplace_back(obj_path_mut.replace_extension(_auto_output_ext));

                    _out_streams.emplace_back(new std::ofstream);
                    dynamic_cast<std::ofstream *>(_out_streams[i - 1].get())->open(
                            obj_path_mut.replace_extension(_auto_output_ext));
                }
                // Misc
                // TODO: make sure every file extension matches pattern
                _input_ext = fs_obj.path().extension();
                _seek_poses.emplace_back(0);
            }
        }
        // safety checks
        assert(i == _inputs.size());
        assert(i == _in_streams.size());
        assert(i == _seek_poses.size());
        if (!_suppress_output) {
            assert(i == _outputs.size());
            assert(i == _out_streams.size());
        }

        log_info(std::to_string(i) + " files matching pattern found.");

        // throw error if no files matching pattern are found
        if (_inputs.empty()) {
            log_error("Cannot find any files matching " + _input_pattern + " for IO.");
            throw IOAssumptionFailedException();
        }
    }

    template<typename T>
    void InterleavedIOScheduler<T>::from_stdin(std::string &out) {
        // read from pipe, cli, etc
        _from_stdin = true;
        _inputs.emplace_back(std::make_pair(0, "NULL"));
        _in_streams.emplace_back(&std::cin);
        // skip if output is being suppressed
        if (!_suppress_output) {
            _outputs.emplace_back(out);
            _out_streams.emplace_back(new std::ofstream);
            dynamic_cast<std::ofstream *>(_out_streams[0].get())->open(out);
        }
    }

template<typename T>
T InterleavedIOScheduler<T>::parse_single() {
    T data;
    _parser->_parsing_fn(_in_streams[_read_head], &data);

        if (_in_streams[_read_head]->eof()) {
            log_info("File " + _inputs[_read_head].second + " exhausted.");
            throw IOResourceExhaustedException();
        }

        _seek_poses[_read_head] = _in_streams[_read_head]->tellg();

        return data;
    }

    template<typename T>
    std::vector<T> InterleavedIOScheduler<T>::parse_multi(uint64_t n) {
        std::vector<T> data;

    for (uint64_t i = 0; i < n; i++) {
        T datum;
        _parser->_parsing_fn(_in_streams[_read_head], &datum);
        data.push_back(datum);

            if (_in_streams[_read_head]->eof()) {
                log_info("File " + _inputs[_read_head].second + " exhausted.");
                throw IOResourceExhaustedException();
            }
        }

        _seek_poses[_read_head] = _in_streams[_read_head]->tellg();

        return data;
    }

    template<typename T>
    void InterleavedIOScheduler<T>::read_until_done() {
        log_info("Beginning to read until all inputs exhausted.");
        while (!_halt_flag) {
            // parse data point(s) in round robin fashion until all files exhausted
            try {
                // TODO add timeout to reading
                _storage_subsystem->insert(std::make_pair(_inputs[_read_head].first, parse_single()));
            } catch (IOResourceExhaustedException &iosee) {
                // remove files after fully read
                log_debug("Closing " + _inputs[_read_head].second + " and removing associated resources.");
                _inputs.erase(_inputs.begin() + _read_head);
                if (dynamic_cast<std::ifstream *>(_in_streams[_read_head].get()) != nullptr)
                    dynamic_cast<std::ifstream *>(_in_streams[_read_head].get())->close();
                _in_streams.erase(_in_streams.begin() + _read_head);
                _seek_poses.erase(_seek_poses.begin() + _read_head);
            }

            uint64_t n_files = _inputs.size();
            // move virtual read head to next file
            _read_head = (_inputs.size() == 0) ? 0 : (_read_head + 1) % std::min(_max_io_interleave, n_files);

            // once all files have been read, flush any data remaining in buffers to buckets
            if (_inputs.empty()) {
                // safety checks, make sure all input streams closed correctly
                assert(_in_streams.empty());
                assert(_seek_poses.empty());
                // flush all remaining data to storage module
                _storage_subsystem->flush();
                _halt_flag = true;
                log_info("Reading finished, all inputs exhausted.");
                //throw IOResourceExhaustedException();
            }
        }
    }


    template<typename T>
    void InterleavedIOScheduler<T>::read_until_full() {
        // TODO: fix deadlock caused by flushing an empty buffer then trying to read from it.
        while (!_halt_flag) {
            while (!_storage_empty_flag) {};
            log_info("Storage empty, reading until full.");
            // parse data point(s) in round robin fashion until all files exhausted
            while (!_storage_subsystem->full() && !_inputs.empty()) {
                try {
                    _storage_subsystem->insert(std::make_pair(_inputs[_read_head].first, parse_single()));
                } catch (IOResourceExhaustedException &iosee) {
                    // remove files after fully read
                    _inputs.erase(_inputs.begin() + _read_head);
                    if (dynamic_cast<std::ifstream *>(_in_streams[_read_head].get()) != nullptr)
                        dynamic_cast<std::ifstream *>(_in_streams[_read_head].get())->close();
                    _in_streams.erase(_in_streams.begin() + _read_head);
                    _seek_poses.erase(_seek_poses.begin() + _read_head);
                }
            }
            log_info("Storage full, requests enabled.");

            _storage_full_flag.store(true);
            _storage_empty_flag.store(false);

            uint64_t n_files = _inputs.size();
            // move virtual read head to next file
            _read_head = (_inputs.size() == 0) ? 0 : (_read_head + 1) % std::min(_max_io_interleave, n_files);

            // once all files have been read, flush any data remaining in buffers to buckets
            if (_inputs.empty()) {
                // safety checks, make sure all input streams closed correctly
                assert(_in_streams.empty());
                assert(_seek_poses.empty());
                // flush all remaining data to storage module
                _storage_subsystem->flush();
                _halt_flag = true;
                log_info("Reading finished, all inputs exhausted.");
                //throw IOResourceExhaustedException();
            }
        }
    }

    template<typename T>
    void InterleavedIOScheduler<T>::write_buffer(std::vector<std::pair<uint64_t, PreHashedString>> &_buff) {
        // skip if output is being suppressed
        if (_suppress_output) {
            return;
        }

        if (!_buff.empty()) {
            // write each line in buffer, switching files when necessary
            for (const auto &mtpx_line : _buff) {
                *(_out_streams[mtpx_line.first]) << mtpx_line.second;
            }
        }
    }

    template<typename T>
    void
    InterleavedIOScheduler<T>::write_buffer_multiplexed(
            std::vector<std::pair<uint64_t, PreHashedString>> &_multiplexed_buff) {
        // skip if output is being suppressed
        if (_suppress_output) {
            return;
        }

        if (!_multiplexed_buff.empty()) {
            // put buffer in order of files to make writing more efficient
            std::sort(_multiplexed_buff.begin(), _multiplexed_buff.end(),
                      [](const std::pair<uint64_t, PreHashedString> &a, const std::pair<uint64_t, PreHashedString> &b) {
                          return a.first < b.first;
                      });

            // write each line in buffer, switching files when necessary
            for (const auto &mtpx_line : _multiplexed_buff) {
                *(_out_streams[mtpx_line.first]) << mtpx_line.second;
            }
        }
    }

    template<typename T>
    bool InterleavedIOScheduler<T>::begin_reading() {
        // spawn reading daemon
        if (!_reading) {
            if (_async_fill_flag) {
                std::thread([&]() { this->read_until_done(); }).detach();
                _storage_full_flag.store(true);
                _storage_empty_flag.store(false);
            } else {
                std::thread([&]() { this->read_until_full(); }).detach();
            }
        }
        _reading.store(true);
        return _reading;
    }

    template<typename T>
    bool InterleavedIOScheduler<T>::stop_reading() {
        log_info("Reading stopped by external call.");
        // stop reading daemon
        _halt_flag = true;
        _reading = false;
        return true;
    }

    template<typename T>
    std::unique_ptr<std::vector<std::pair<uint64_t, T> > > InterleavedIOScheduler<T>::request_bucket() {
        if (_async_fill_flag) {
            if (!_storage_subsystem->empty() || !_inputs.empty()) { // only wait for buckets to fill if async
                return _storage_subsystem->next_bucket();
            } else {
                log_debug("Bucket storage empty. Stopping bucket requests.");
                throw RequestToEmptyStorageException();
            }
        } else {
            // request a bucket sequentially
            if (!_storage_subsystem->empty()) {
                return _storage_subsystem->next_bucket();
            } else {
                if (!_storage_subsystem->empty() || !_inputs.empty()) {
                    _storage_full_flag.store(false);
                    _storage_empty_flag.store(true);

                    while (!_storage_full_flag && !_halt_flag) {}; // wait until full if empty

                    return _storage_subsystem->next_bucket();
                } else {
                    throw RequestToEmptyStorageException();
                }
            }
        }
    }

    template<typename T>
    std::future<std::vector<std::pair<uint64_t, T> > > InterleavedIOScheduler<T>::request_bucket_async() {
        while (!_storage_full_flag) {};
        if (!_storage_subsystem->empty() ||
            (_async_fill_flag && !_inputs.empty())) { // only wait for buckets to fill if async
            return std::move(_storage_subsystem->next_bucket_async());
        } else {
            _storage_empty_flag.store(true);
            _storage_full_flag.store(false);
            //throw RequestToEmptyStorageException();
        }
    }

    template<typename T>
    void InterleavedIOScheduler<T>::write_async(uint64_t out_ind, PreHashedString &line) {
        // skip if output is being suppressed
        if (_suppress_output) {
            return;
        }

        _out_buff.emplace_back(std::make_pair(out_ind, line + "\n"));
        if (_out_buff.size() >= _out_buff_threshold) {
            if (_outputs.size() > 1) {
                write_buffer_multiplexed(_out_buff);
            } else {
                write_buffer(_out_buff);
            }
            _out_buff.clear();
            _out_buff.reserve(_out_buff_threshold);
            //std::thread([&](){write_buffer(_out_buff);}).detach();
        }
    }

    template<typename T>
    void InterleavedIOScheduler<T>::flush() {
        // skip if output is being suppressed
        if (_suppress_output) {
            return;
        }

        write_buffer(_out_buff);
    }

    template<typename T>
    std::vector<std::string> InterleavedIOScheduler<T>::get_input_filenames() {
        std::vector<std::string> out;
        for (const auto &file : _inputs) {
            out.emplace_back(file.second);
        }
        return out;
    }

    template<typename T>
    InterleavedIOScheduler<T> &InterleavedIOScheduler<T>::operator=(const InterleavedIOScheduler<T> &&other) {
        log_debug("Moving IO module.");
        // IO handles
        _inputs = other._inputs; // pair of unique file id and file path
        _seek_poses = other._seek_poses;
        _outputs = other._outputs;
        _read_head = other._read_head;

        // IO buffers
        _storage_subsystem = other._storage_subsystem;
        _out_buff = other._out_buff;

        // Effort limits
        _max_io_interleave = other._max_io_interleave;
        _out_buff_threshold = other._out_buff_threshold;
        _max_wait_time = other._max_wait_time;

        // Flags
        _from_stdin = other._from_stdin;
        _halt_flag.store(other._halt_flag.load());
        _async_fill_flag.store(other._async_fill_flag);
        _storage_full_flag.store(other._storage_full_flag);
        _storage_empty_flag.store(other._storage_empty_flag);
        _reading.store(other._reading);
        _suppress_output.store(other._suppress_output);

        // Automated file formatting variables
        _input_pattern = other._input_pattern;
        _input_ext = other._input_ext;
        _auto_output_ext = other._auto_output_ext; // used if modifying extension of input to generate output

        // Parser
        _parser = other._parser;

        // delete other and open up new streams (cannot be copied)
        // delete other;
        if (!_from_stdin) {
            //
            for (const auto &f : _inputs) {
                _in_streams.emplace_back(new std::ifstream);
                dynamic_cast<std::ifstream *>(_in_streams.back().get())->open(f.second);
            }

            for (const auto &f : _outputs) {
                _out_streams.emplace_back(new std::ofstream);
                dynamic_cast<std::ofstream *>(_out_streams.back().get())->open(f);
            }
        } else {
            // read from pipe, cli, etc
            _in_streams.emplace_back(&std::cin);
            _out_streams.emplace_back(new std::ofstream);
            dynamic_cast<std::ofstream *>(_out_streams[0].get())->open(_outputs.front());
        }
        log_debug("IO module moved.");
    }
}

#endif //SEALM_IO_HPP
