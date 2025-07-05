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

#include <string_view>
#include <utility>
#include <vector>

namespace secondary_index {

namespace {

logging::logger logger{"index_options"};

// A map of valid index options. It does NOT contain any option that is valid for the underlying MV.
// Those are supposed to be stored somewhere else.
//
// FIXME: Change to std::flat_map.
const std::vector<std::pair<index_type, std::vector<std::string_view>>> valid_index_options = {
    {index_type::regular, {
        // None.
    }},
    {index_type::vector, {
        "similarity_function",
        "maximum_node_connections",
        "construction_beam_width",
        "search_beam_width"
    }}
};

std::span<const std::string_view> get_index_options(index_type type) {
    for (auto&& [index_type, index_opts] : valid_index_options) {
        if (index_type == type) {
            return index_opts;
        }
    }

    // Casting to size_t to avoid having to deal with formatting one-byte types...
    size_t enum_value = std::to_underlying(type);
    on_internal_error(logger, seastar::format("There are no options for index_type of enum value equal to {}", enum_value));
}

} // anonymous namespace

std::expected<index_options, sstring> filter_options(index_type type, const std::unordered_map<sstring, sstring>& options) {
    index_options result {.type = type};

    const auto index_opts = get_index_options(type);
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

} // namespace secondary_index
