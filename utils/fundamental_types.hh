/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <expected>
#include <optional>
#include <string_view>
#include <system_error>

namespace utils {

std::optional<bool> parse_bool(std::string_view value) noexcept;
bool parse_bool(std::string_view value, bool default_value) noexcept;

std::expected<int, std::errc> parse_int(std::string_view value) noexcept;
int parse_int(std::string_view value, int default_value) noexcept;

std::expected<long, std::errc> parse_long(std::string_view value) noexcept;
long parse_long(std::string_view value, long default_value) noexcept;

std::expected<double, std::errc> parse_double(std::string_view value) noexcept;
double parse_double(std::string_view value, double default_value) noexcept;

} // namespace utils
