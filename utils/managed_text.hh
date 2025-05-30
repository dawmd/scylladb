
/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "utils/fragment_range.hh"
#include "utils/managed_bytes.hh"

#include <compare>

class text_ostream;
class managed_text;

template <mutable_view is_mutable>
class managed_text_basic_view : private managed_bytes_basic_view<is_mutable> {
private:
    friend managed_text;

    using base_view = managed_bytes_basic_view<is_mutable>;

public:
    using value_type = typename base_view::value_type;
    using value_type_maybe_const = std::conditional_t<is_mutable == mutable_view::yes, value_type, const value_type>;

public:
    using base_view::size;
    using base_view::size_bytes;
    using base_view::empty;
    using base_view::current_fragment;
    using base_view::remove_prefix;
    using base_view::remove_current;

    managed_text_basic_view prefix(size_t len) const {
        return managed_text_basic_view(base_view::prefix(len));
    }

    managed_text_basic_view substr(size_t offset, size_t len) const {
        return managed_text_basic_view(base_view::substr(offset, len));
    }
};

// A thin wrapper over `managed_bytes` introducing a contract between this interface
// and its user: the bytes managed by an instance of `managed_text` are GUARANTEED to
// represent unencoded text.
//
// This type is supposed to function as a complement of `managed_bytes`.
class managed_text : private managed_bytes {
private:
    friend text_ostream;

    managed_text(managed_bytes mbs)
        : managed_bytes(std::move(mbs))
    {}

public:
    managed_text() = default;

    managed_text(std::string_view content)
        : managed_bytes(reinterpret_cast<const int8_t*>(content.data()), content.length())
    {}

    // Should we have this one???
    managed_text(initialized_later, size_type size)
        : managed_bytes(initialized_later{}, size)
    {}

    using managed_bytes::operator==;

    std::strong_ordering operator<=>(const managed_text& other) const {
        return compare_unsigned(managed_bytes_view(*this), managed_bytes_view(other));
    }

    char& operator[](size_type index) {
        return reinterpret_cast<char&>(managed_bytes::operator[](index));
    }
    const char& operator[](size_type index) const {
        return reinterpret_cast<const char&>(managed_bytes::operator[](index));
    }

    using managed_bytes::size;
    using managed_bytes::empty;
    using managed_bytes::external_memory_usage;
    using managed_bytes::minimal_external_memory_usage;
};
