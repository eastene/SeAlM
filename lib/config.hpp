//
// Created by evan on 5/9/19.
//

#ifndef ALIGNER_CACHE_CONFIG_HPP
#define ALIGNER_CACHE_CONFIG_HPP

#include <experimental/filesystem>
#include <unordered_map>
#include <fstream>
#include <algorithm>

class ConfigNotFoundException : public std::exception{};
class ConfigOverwrittenException : public std::exception{};
class InvalidConversionException : public std::exception{};

class ConfigParser{
private:
    // State variables
    std::experimental::filesystem::path _config_path;
    std::unordered_map<std::string, std::string> _configs;

    // Configuration parsing parameters
    std::string _config_extension = ".ini";
    char _comment = '#';
    char _delimiter = '=';
    bool _err_on_repeat = false;
    bool _case_insensitive = true;

    void parse();
public:

    explicit ConfigParser(std::experimental::filesystem::path &config_path): _config_path{config_path}{parse();};

    std::string get_val(const std::string &key);

    long get_long_val(const std::string &key);

    double get_double_val(const std::string &key);

};

void ConfigParser::parse() {
    std::ifstream config_file(_config_path);

    if (config_file && _config_path.extension().string() == _config_extension) {
        for(std::string line; std::getline(config_file, line);){
            // remove whitespace
            line.erase(std::remove_if(line.begin(), line.end(), isspace), line.end());
            // ignore comment lines
            if (line[0] == _comment|| line.empty()) {
                continue;
            }
            // split string on delimiter into key-value pair
            auto delimiter = line.find(_delimiter);
            auto key = line.substr(0, delimiter);
            auto value = line.substr(delimiter + 1);

            if (_case_insensitive) {
                std::transform(key.begin(), key.end(), key.begin(), ::tolower);
                std::transform(value.begin(), value.end(), value.begin(), ::tolower);
            }
            if (_configs.find(key) != _configs.end() && _err_on_repeat) {
                config_file.close();
                throw ConfigOverwrittenException();
            }
            _configs.insert_or_assign(key, value);
        }
    } else {
        config_file.close();
        throw ConfigNotFoundException();
    }
}

std::string ConfigParser::get_val(const std::string &key) {
    return _configs.find(key) != _configs.end() ? _configs[key] : "";
}

long ConfigParser::get_long_val(const std::string &key) {
    try {
        if (_configs.find(key) != _configs.end())
            return std::stol(_configs[key]);
        else
            return std::numeric_limits<long>::max();
    } catch(std::invalid_argument &ia) {
        throw InvalidConversionException();
    }
}

double ConfigParser::get_double_val(const std::string &key) {
    try {
        if (_configs.find(key) != _configs.end())
            return std::stod(_configs[key]);
        else
            return std::numeric_limits<double>::max();
    } catch(std::invalid_argument &ia) {
        throw InvalidConversionException();
    }
}

#endif //ALIGNER_CACHE_CONFIG_HPP
