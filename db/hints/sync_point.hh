/*
 * Copyright (C) 2021-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/sstring.hh>

#include "bytes.hh"
#include "db/commitlog/replay_position.hh"
#include "locator/host_id.hh"

#include <cstdint>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace db::hints {

/// A sync point is a collection of positions in hint queues. It can be waited on.
///
/// A sync point corresponds to exactly one type of a hint manager.
struct sync_point {
    using shard_rps = std::unordered_map<locator::host_id, db::replay_position>;

    // ID of the host which created this sync point
    locator::host_id host_id;
    std::vector<shard_rps> regular_per_shard_rps;
    std::vector<shard_rps> mv_per_shard_rps;

    /// \brief Decodes a sync point from an encoded, textual form (a hexadecimal string).
    static sync_point decode(sstring_view s);

    /// \brief Encodes the sync point in a textual form (a hexadecimal string).
    seastar::sstring encode() const;

    bool operator==(const sync_point& other) const = default;
};

std::ostream& operator<<(std::ostream& out, const sync_point& sp);

// IDL type
// Contains per-endpoint and per-shard information about replay positions
// for a particular type of hint queues (regular mutation hints or MV update hints)
struct per_manager_sync_point_v1 {
    std::vector<locator::host_id> hosts;
    std::vector<db::replay_position> flattened_rps;
};

// IDL type
struct sync_point_v1 {
    locator::host_id host_id;
    uint16_t shard_count;

    // Sync point information for regular mutation hints
    db::hints::per_manager_sync_point_v1 regular_sp;

    // Sync point information for materialized view hints
    db::hints::per_manager_sync_point_v1 mv_sp;
};

} // namespace db::hints
