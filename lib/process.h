//
// Created by Evan on 4/13/2020.
//

#ifndef SEALM_PROCESS_H
#define SEALM_PROCESS_

#include <string>
#include <cpp-subprocess/subprocess.hpp>

/*
 *
 * SUB-PROCESS ADAPTER
 *
 */

// Adapter for sub-process.cpp (https://github.com/arun11299/cpp-subprocess)
class SubProccessAdapter {
private:
    // process flags
    bool _interactive;
    bool _open;

    // process
    auto _proc;

protected:
    // sub-process methods (to be called by derived classes)
    template <typename... Args>
    void popen(std::string &command) {
        if (_interactive && _open) {
            return;
        }

        auto proc = subprocess::Popen({command}, subprocess::input{subprocess::PIPE},
                                                    subprocess::output{subprocess::PIPE},
                                                    subprocess::error{subprocess::PIPE});
        _proc = proc;
    }

    void communicate_and_parse(std::stringstream &in, std::vector<std::string> *out) {
        auto res = _proc.communicate(in.str().c_str(), in.str().size());
        uint64_t k = 0;

        std::stringstream out_ss;
        out_ss << res.first.buf.data();
        for (std::string line; std::getline(out_ss, line);) {
            (*out)[k++] = line;
        }
    }

public:
    SubProccessAdapter() : _interactive{false}, _open{false} {};

    SubProccessAdapter(bool interactive) : _interactive{interactive}, _open{false} {};

    void set_interactivity(bool interactive) { _interactive = interactive; }
};

#endif //SEALM_PROCESS_H
