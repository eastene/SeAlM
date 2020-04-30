//
// Created by Evan on 4/30/2020.
//

#ifndef SEALM_STRING_H
#define SEALM_STRING_H

#include <iostream>
#include <string>

namespace SeAlM {

    class PreHashedString {
    protected:
        std::hash<std::string> _hash;
        std::string _str;
        size_t _pre_comp_hash;
        bool _hashed;
    public:
        PreHashedString() : _pre_comp_hash{0}, _hashed{false} {}

        PreHashedString(std::string s) { set_string(s); }

        PreHashedString(const char *s) { set_string(s); }

        PreHashedString& operator=(const PreHashedString &other) {
            _hash = other._hash;
            _str = other._str;
            _pre_comp_hash = other._pre_comp_hash;
            _hashed = other._hashed;
        }

        /*
         * Hash Functions
         */

        [[nodiscard]] bool has_hash() const { return _hashed; }

        void set_hash() {
            _pre_comp_hash = _hash(_str);
            _hashed = true;
        }

        [[nodiscard]] size_t get_hash() const { return _pre_comp_hash; }

        void set_string(std::string &other, bool comp_hash = true) {
            _str = other;
            _str.shrink_to_fit();
            if (comp_hash)
                set_hash();
        }

        void set_string(const char *other, bool comp_hash = true) {
            _str = other;
            _str.shrink_to_fit();
            if (comp_hash)
                set_hash();
        }

        /*
         * String Functions
         */

        size_t size() const { return _str.size(); }

        std::string substr(unsigned long pos, unsigned long n) { return _str.substr(pos, n); }

        size_t find(const char _s) { return _str.find(_s); }

        char operator[](size_t i) const {
            return _str[i];
        }

        PreHashedString operator+ (PreHashedString &other) const {
            return _str + other._str;
        }

        PreHashedString operator+ (std::string s) const {
            return _str + s;
        }

        /*
         * Misc Functions
         */

        bool operator == (PreHashedString other) const {
            // TODO: check for hash collisions
            return _pre_comp_hash == other._pre_comp_hash;
        }

        bool operator < (PreHashedString other) const {
            return _str < other._str;
        }

        friend std::istream &operator>>(std::istream &in, PreHashedString &s){
            in >> s._str;
            s._str.shrink_to_fit();
            s.set_hash();
            return in;
        }

        friend std::ostream &operator<<(std::ostream &out, const PreHashedString &s) {
            out << s._str;
            return out;
        }
    };

//    std::istream &operator>>(std::istream &in, PreHashedString &s) {
//        in >> s._str;
//        s._str.shrink_to_fit();
//        s.set_hash();
//        return in;
//    }
//
//    std::ostream &operator<<(std::ostream &out, const PreHashedString &s) {
//        out << s._str;
//        return out;
//    }
}

namespace std {
    template<>
    struct std::hash<SeAlM::PreHashedString> {
        size_t operator()(const SeAlM::PreHashedString &s) const {
            if (!s.has_hash()) {
                // TODO: make more explicit exception
                std::exception e;
                throw e;
            }
            return s.get_hash();
        }
    };
}

#endif //SEALM_STRING_H
