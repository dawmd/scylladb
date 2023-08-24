/*
 * Modified by ScyllaDB
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

// Seastar features.
#include <seastar/util/bool_class.hh>

// Scylla includes.
#include "gms/inet_address.hh"

// STD.
#include <cstdint>

namespace db::hints {
namespace internal {

// Type identifying host a specific subset of hints should be sent to.
using host_id_type = gms::inet_address;

// Tag specifying if hint sending should enter the so-called "drain mode".
// If it should, that means that if a failure while sending a hint occurs,
// the hint will be ignored rather than tried to attempted again.
//
// The tag is useful in communication between host managers
// and data structures responsible for sending hints, e.g. hint sender.
using drain = seastar::bool_class<class drain_tag>;

// Statistics related to hint sending. They should collect information
// about a whole shard. Compare `manager.hh` in the hints module
// and `hint_sender.hh` in this directory.
struct hint_stats {
    uint64_t size_of_hints_in_progress  = 0;
    uint64_t written                    = 0;
    uint64_t errors                     = 0;
    uint64_t dropped                    = 0;
    uint64_t sent                       = 0;
    uint64_t discarded                  = 0;
    uint64_t corrupted_files            = 0;
};

} // namespace internal
} // namespace db::hints
