/*
 * Copyright (C) 2025-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include <seastar/core/sstring.hh>

#include <expected>
#include <map>
#include <unordered_map>

using namespace seastar;

namespace secondary_index {

enum class index_type {
    regular,    // A non-custom, global or local secondary index, with an underlying MV.
    vector      // Custom vector index.
};

/// Depending on the type of an index, it's configured by different options.
///
/// Example 1: vector index accepts `similarity_function` as one of its options,
///            but a regular global secondary index does not.
/// Example 2: a regular global secondary index will accept `synchronous_updates`
///            as its option (and apply it to its underlying materialized view),
///            but it's invalid for vector index as it doesn't utilize MVs in any way.
///
/// This struct is supposed to be a wrapper over filtered options. The goal is to provide
/// a convenient access to them for validation reasons, e.g. when processing a request to
/// create or alter an index.
struct index_options {
    index_type type;
    // Options corresponding to the index itself.
    // The map should contain all valid options that are applicable to an index directly.
    // It should NOT contain ANY option that is only applicable to the underlying MV.
    std::map<sstring, sstring> idx_options;
    // Options corresponding to the underlying MV of the index.
    std::map<sstring, sstring> mv_options;
};

// Return filtered options. If at least one of the options is invalid, return its name.
//
// FIXME: It would be nice to accept a broader set of ranges here.
std::expected<index_options, sstring> filter_options(index_type, const std::unordered_map<sstring, sstring>& options);

// Returns an exception message if validation fails.
std::expected<void, sstring> validate_index_options(const index_options&);

} // namespace secondary_index
