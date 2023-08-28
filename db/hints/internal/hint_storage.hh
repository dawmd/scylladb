/*
 * Modified by ScyllaDB
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

// Seastar features.
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

// Scylla includes.
#include "db/commitlog/commitlog.hh"
#include "db/commitlog/commitlog_entry.hh"
#include "db/hints/internal/common.hh"
#include "utils/loading_shared_values.hh"

// STD.
#include <chrono>
#include <string>
#include <string_view>

/// This file is supposed to gather meta information about data structures
/// and types related to storing hints on disk.
/// As of now, commitlog is used for this purpose, but if more ideas pop up
/// in the future, this file should reflect them accordingly.

namespace db::hints {
namespace internal {

// TODO: I don't understand these scary names yet, but they should be described properly
//       or renamed to something easier to understand.
using node_to_hint_store_factory_type = utils::loading_shared_values<host_id_type, commitlog>;
using hint_store_ptr = typename node_to_hint_store_factory_type::entry_ptr;
using hint_entry_reader = commitlog_entry_reader;

// TODO: Changet his to `constexpr std::string` once std::string supports constexpr constructors.
inline const std::string HINT_FILENAME_PREFIX{"HintsLog" + commitlog::descriptor::SEPARATOR};
constexpr inline std::chrono::seconds HINT_FILE_WRITE_TIMEOUT = std::chrono::seconds(2);

/// \brief Rebalance hints segments among all present shards.
///
/// The difference between the number of segments on every two shard will be not greater than 1 after the
/// rebalancing.
///
/// Removes the sub-directories of \ref hints_directory that correspond to shards that are not relevant any more
/// (re-sharding to a lower shards number case).
///
/// Complexity: O(N+K), where N is a total number of present hints' segments and
///                           K = <number of shards during the previous boot> * <number of end points for which hints where ever created>
///
/// \param hints_directory A hints directory to rebalance
/// \return A future that resolves when the operation is complete.
seastar::future<> rebalance_hints(seastar::sstring hint_directory);

inline seastar::future<> rebalance_hints(std::string_view hint_directory) {
    return rebalance_hints(seastar::sstring{hint_directory});
}

} // namespace internal
} // namespace db::hints
