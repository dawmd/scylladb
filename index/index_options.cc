/*
 * Copyright (C) 2025-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#include "db/view/view_options.hh"
#include "index/index_options.hh"
#include "utils/log.hh"

#include <seastar/core/on_internal_error.hh>

#include <ranges>
#include <string_view>
#include <utility>
#include <vector>

namespace secondary_index {

namespace {

logging::logger logger{"index_options"};

template <int MAX>
std::expected<void, sstring> validate_unsigned_option(std::string_view value) {
    int num_value;
    size_t len;
    try {
        num_value = std::stoi(std::string(value), &len);
    } catch (...) {
        return std::unexpected(seastar::format("Numeric option {} is not a valid number", value));
    }
    if (len != value.size()) {
        return std::unexpected(seastar::format("Numeric option {} is not a valid number", value));
    }

    if (num_value < 0 || num_value > MAX) {
        return std::unexpected(seastar::format("Numeric option {} out of valid range [0 - {}]", value, MAX));
    }

    return {};
}

std::expected<void, sstring> validate_similarity_function(std::string_view value) {
    if (value != "COSINE" && value != "EUCLIDEAN" && value != "DOT_PRODUCT") {
        return std::unexpected(seastar::format("Unsupported similarity function: {}", value));
    }

    return {};
}

using option_validator = std::function<std::expected<void, sstring>(std::string_view)>;

struct index_option {
    std::string_view name;
    option_validator validator;
};

// A map of valid index options. It does NOT contain any option that is valid for the underlying MV.
// Those are supposed to be stored somewhere else.
//
// FIXME: Change to std::flat_map.
const std::vector<std::pair<
        index_type,
        std::vector<index_option>
>> valid_index_options = {
    {index_type::regular, {
        // None.
    }},
    {index_type::vector, {
        {"similarity_function", validate_similarity_function},
        {"maximum_node_connections", validate_unsigned_option<512>},
        {"construction_beam_width", validate_unsigned_option<4096>},
        {"search_beam_width", validate_unsigned_option<4096>},
    }}
};

const auto& get_index_options(index_type type) {
    for (auto&& [index_type, options] : valid_index_options) {
        if (index_type == type) {
            return options;
        }
    }

    // Casting to size_t to avoid having to deal with formatting one-byte types...
    size_t enum_value = std::to_underlying(type);
    on_internal_error(logger, seastar::format("There are no options for index_type of enum value equal to {}", enum_value));
}

auto get_index_option_names(index_type type) {
    return get_index_options(type) | std::views::transform([] (const index_option& opt) {
        return opt.name;
    });
}

std::expected<void, sstring> validate_vector_index_options(const index_options& opts) {
    const auto& options = get_index_options(opts.type);

    for (const auto& [key, value] : opts.idx_options) {
        auto it = std::ranges::find(options, key, std::mem_fn(&index_option::name));
        if (it == std::ranges::end(options)) {
            size_t enum_value = std::to_underlying(opts.type);
            on_internal_error(logger, seastar::format("Didn't find validator for type={}, opt name={}", enum_value, key));
        }
        auto result = it->validator(value);
        if (!result.has_value()) {
            return result;
        }
    }

    return {};
}

} // anonymous namespace

std::expected<index_options, sstring> filter_options(index_type type, const std::unordered_map<sstring, sstring>& options) {
    index_options result {.type = type};

    const auto index_opts = get_index_option_names(type);
    for (const auto& [key, value] : options) {
        if (std::ranges::find(index_opts, key) != std::ranges::end(index_opts)) {
            result.idx_options.emplace(std::make_pair(key, value));
            continue;
        }
        if (db::view::is_view_option(key)) {
            result.mv_options.emplace(std::make_pair(key, value));
            continue;
        }
        
        return std::unexpected(key);
    }

    return result;
}

std::expected<void, sstring> validate_index_options(const index_options& opts) {
    switch (opts.type) {
        case index_type::regular:
            // Nothing to check.
            return {};
        case index_type::vector:
            return validate_vector_index_options(opts);
    }
}

} // namespace secondary_index
