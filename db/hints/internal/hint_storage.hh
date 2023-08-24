/*
 * Modified by ScyllaDB
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

// Scylla includes.
#include "db/commitlog/commitlog.hh"
#include "db/commitlog/commitlog_entry.hh"
#include "db/hints/internal/common.hh"
#include "utils/loading_shared_values.hh"

// STD.
#include <string>

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

} // namespace internal
} // namespace db::hints
