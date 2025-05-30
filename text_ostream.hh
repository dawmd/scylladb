/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "bytes_ostream.hh"
#include "utils/managed_text.hh"

// A thin wrapper over `bytes_ostream` introducing a contract between this interface
// and its user: the bytes managed by an instance of `text_ostream` are GUARANTEED to
// represent unencoded text.
//
// This type is supposed to function as a complement of `bytes_ostream`, which represents
// a generic stream of bytes.
class text_ostream : public bytes_ostream {
private:
    using bytes_ostream::bytes_ostream;
    using bytes_ostream::operator=;

    using bytes_ostream::write_place_holder;
    using bytes_ostream::write;
    using bytes_ostream::view;
    using bytes_ostream::append;

public:
    text_ostream() = default;
    explicit text_ostream(size_t initial_chunk_size) noexcept : bytes_ostream(initial_chunk_size) {}
    text_ostream(const text_ostream& o) : bytes_ostream(o) {}
    text_ostream(text_ostream&& o) noexcept : bytes_ostream(std::move(o)) {}

    text_ostream& operator=(const text_ostream& o) {
        bytes_ostream::operator=(o);
        return *this;
    }
    text_ostream& operator=(text_ostream&& o) {
        bytes_ostream::operator=(std::move(o));
        return *this;
    }

    [[gnu::always_inline]]
    void write(std::string_view sv) {
        bytes_ostream::write(sv.data(), sv.length());
    }

    std::string_view view() const {
        bytes_view bv = bytes_ostream::view();
        return std::string_view(reinterpret_cast<const char*>(bv.data()), bv.length());
    }

    void append(const text_ostream& o) {
        bytes_ostream::append(static_cast<const bytes_ostream&>(o));
    }

    managed_text to_managed_text() && {
        auto& self = *this;
        return managed_text(std::move(self).to_managed_bytes());
    }

    bytes_ostream to_bytes_ostream() && {
        auto& self = *this;
        return bytes_ostream(std::move(self));
    }
};
