/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "utils/fundamental_types.hh"

#include <algorithm>
#include <cctype>
#include <concepts>
#include <charconv>

namespace utils {

namespace {

template <typename T>
    requires std::integral<T> || std::floating_point<T>
std::expected<T, std::errc> parse_value(std::string_view value) noexcept {
    T result;
    const char* first = value.begin();
    const char* last = value.end();

    const auto [ptr, ec] = std::from_chars(first, last, result);

    if (ptr == last && ec == std::errc{}) {
        return result;
    }

    return std::unexpected(ec);
}

} // anonymous namespace

std::optional<bool> parse_bool(std::string_view value) noexcept {
    auto cmp = [&] (std::string_view expected) -> bool {
        auto project_to_lower = [] (char c) noexcept {
            return static_cast<char>(std::tolower(c));
        };
        return std::ranges::equal(value, expected, {}, project_to_lower);
    };

    constexpr std::string_view valid_true[] = {"true", "1", "yes"};
    constexpr std::string_view valid_false[] = {"false", "0", "no"};

    if (std::ranges::any_of(valid_true, cmp)) {
        return true;
    }
    if (std::ranges::any_of(valid_false, cmp)) {
        return false;
    }

    return std::nullopt;
}
bool parse_bool(std::string_view value, bool default_value) noexcept {
    return parse_bool(value).value_or(default_value);
}

std::expected<int, std::errc> parse_int(std::string_view value) noexcept {
    return parse_value<int>(value);
}
int parse_int(std::string_view value, int default_value) noexcept {
    return parse_int(value).value_or(default_value);
}

std::expected<long, std::errc> parse_long(std::string_view value) noexcept {
    return parse_value<long>(value);
}
long parse_long(std::string_view value, long default_value) noexcept {
    return parse_long(value).value_or(default_value);
}

std::expected<double, std::errc> parse_double(std::string_view value) noexcept {
    return parse_value<double>(value);
}
double parse_double(std::string_view value, double default_value) noexcept {
    return parse_value<double>(value).value_or(default_value);
}

} // namespace utils
