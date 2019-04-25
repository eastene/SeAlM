//
// Created by evan on 4/22/19.
//

#ifndef ALIGNER_CACHE_TYPES_HPP
#define ALIGNER_CACHE_TYPES_HPP

#include <vector>
#include <string>
#include <chrono>
#include <variant>

typedef std::vector<std::string> Read;
typedef std::chrono::milliseconds Mills;
typedef std::tuple<std::variant<std::string, uint32_t>, std::string, std::string> RedupeRef;

#endif //ALIGNER_CACHE_TYPES_HPP
