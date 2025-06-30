/*
 * Copyright (C) 2014-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "bytes.hh"
#include <fmt/ostream.h>
#include <seastar/core/format.hh>
#include "bytes_ostream.hh"

static inline int8_t hex_to_int(unsigned char c) {
    switch (c) {
        case '0': return 0;
        case '1': return 1;
        case '2': return 2;
        case '3': return 3;
        case '4': return 4;
        case '5': return 5;
        case '6': return 6;
        case '7': return 7;
        case '8': return 8;
        case '9': return 9;
        case 'a': case 'A': return 10;
        case 'b': case 'B': return 11;
        case 'c': case 'C': return 12;
        case 'd': case 'D': return 13;
        case 'e': case 'E': return 14;
        case 'f': case 'F': return 15;
        default:
            return -1;
    }
}

bytes from_hex(std::string_view s) {
    if (s.length() % 2 == 1) {
        throw std::invalid_argument("An hex string representing bytes must have an even length");
    }
    bytes out{bytes::initialized_later(), s.length() / 2};
    unsigned end = out.size();
    for (unsigned i = 0; i != end; i++) {
        auto half_byte1 = hex_to_int(s[i * 2]);
        auto half_byte2 = hex_to_int(s[i * 2 + 1]);
        if (half_byte1 == -1 || half_byte2 == -1) {
            throw std::invalid_argument(fmt::format("Non-hex characters in {}", s));
        }
        out[i] = (half_byte1 << 4) | half_byte2;
    }
    return out;
}

sstring to_hex(bytes_view b) {
    return fmt::to_string(fmt_hex(b));
}

sstring to_hex(const bytes& b) {
    return to_hex(bytes_view(b));
}

sstring to_hex(const bytes_opt& b) {
    return !b ? "null" : to_hex(*b);
}


void bytes_ostream::write(value_type byte) {
    if (current_space_left()) {
        _current->data[_current->frag_size++] = byte;
        _size += 1;
    } else {
        alloc_new(1);
        _current->data[_current->frag_size] = byte;
        // auto* chunk = alloc_new(1);
        // chunk[0] = byte;
    }
}

namespace std {

std::ostream& operator<<(std::ostream& os, const bytes_view& b) {
    fmt::print(os, "{}", fmt_hex(b));
    return os;
}

}
