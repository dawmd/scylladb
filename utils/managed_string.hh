
/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "utils/managed_bytes.hh"
#include "bytes_ostream.hh"

// A thin wrapper over `managed_bytes` representing a fragmented UTF-8 encoded string.
class managed_string {
private:
    managed_bytes _impl;

private:
    managed_string(managed_bytes mb) : _impl(std::move(mb)) {}

public:
    managed_string(managed_string&&) noexcept = default;
    managed_string& operator=(managed_string&&) noexcept = default;

    // Precondition: the passed argument must represent a valid UTF-8 string.
    static managed_string from_managed_bytes_unsafe(managed_bytes mb) {
        return managed_string(std::move(mb));
    }

    bool operator==(const managed_string&) const = default;

    std::strong_ordering operator<=>(const managed_string& other) const {
        auto lv = managed_bytes_view(_impl);
        auto rv = managed_bytes_view(other._impl);
        return compare_unsigned(lv, rv);
    }

    template <typename Self>
    decltype(auto) as_managed_bytes(this Self&& self) {
        return managed_bytes(std::forward_like<Self>(self._impl));
    }
};

// A thin wrapper over `bytes_ostream` with a promise that it corresponds
// to actual UTF-8 characters, not just generic bytes.
class fragmented_ostringstream {
private:
    bytes_ostream _impl;

public:
    void write(std::string_view sv) {
        _impl.write(sv.data(), sv.size());
    }
    fragmented_ostringstream& operator<<(std::string_view sv) {
        write(sv);
        return *this;
    }

    managed_string to_managed_string() && {
        return managed_string::from_managed_bytes_unsafe(std::move(_impl).to_managed_bytes());
    }
};
