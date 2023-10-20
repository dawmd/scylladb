/*
 * Modified by ScyllaDB
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

// Seastar features.
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/timer.hh>
#include <seastar/util/noncopyable_function.hh>

// Scylla includes.
#include "db/commitlog/commitlog.hh"
#include "db/commitlog/commitlog_entry.hh"
#include "db/hints/internal/common.hh"
#include "mutation/frozen_mutation.hh"
#include "tracing/trace_state.hh"
#include "utils/loading_shared_values.hh"

// STD.
#include <filesystem>
#include <utility>

/// This file is supposed to gather meta information about data structures
/// and types related to storing hints.
///
/// Under the hood, commitlog is used for managing, storing, and reading
/// hints from disk.

namespace db::hints {
namespace internal {

using node_to_hint_store_factory_type = utils::loading_shared_values<endpoint_id, db::commitlog>;
using hints_store_ptr = node_to_hint_store_factory_type::entry_ptr;
using hint_entry_reader = commitlog_entry_reader;

/// \brief Rebalance hints segments among all present shards.
///
/// The difference between the number of segments on every two shard will not be
/// greater than 1 after the rebalancing.
///
/// Removes the subdirectories of \ref hint_directory that correspond to shards that
/// are not relevant anymore (in the case of re-sharding to a lower shard number).
///
/// Complexity: O(N+K), where N is a total number of present hint segments and
///                           K = <number of shards during the previous boot> * <number of endpoints
///                                 for which hints where ever created>
///
/// \param hint_directory A hint directory to rebalance
/// \return A future that resolves when the operation is complete.
future<> rebalance_hints(std::filesystem::path hint_directory);

constexpr inline size_t HINT_SEGMENT_SIZE_IN_MB = 32;
constexpr inline size_t MAX_HINT_SIZE_PER_ENDPOINT_IN_MB = 128;

class hint_endpoint_storage {
public:
    enum class hint_reader_result {
        CONTINUE_FORGET,
        CONTINUE_KEEP,
        STOP_FORGET,
        STOP_KEEP
    };

    using hint_size_type = size_t;
    using hint_reader_type = noncopyable_function<future<hint_reader_result> (hint_entry_reader, hint_size_type)>;

private:
    using flush_clock_type = seastar::lowres_clock;
    static_assert(noexcept(flush_clock_type::now()), "flush_clock_type::now() must be noexcept");
    using flush_timer_type = timer<flush_clock_type>;

    using timeout_clock_type = seastar::lowres_clock;
    static_assert(noexcept(timeout_clock_type::now()), "timeout_clock_type::now() must be noexcept");

    using segment_list = std::list<sstring>;

private:
    std::unique_ptr<commitlog> _commitlog = nullptr;

    // The position of the last written hint to the underlying commitlog instance.
    // New hints should be written past it.
    replay_position _write_rp{};
    // The position of the first hint that should be read next time.
    replay_position _read_rp{};

    segment_list _segment_list{};
    segment_list _foreign_segment_list{};

    // This scheduling group shouldn't affect the performance of mutations. Storing a hint is performed
    // asynchronously to mutating. Though `store_hint` returns a future, it should be immediately
    // discarded by the caller.
    seastar::scheduling_group _sched_group;

    // Timer used for regular flushing hints to disk.
    flush_timer_type _flush_timer;
    seastar::semaphore _file_update_mutex{1};
    // Used for creating and read from the commitlog instance.
    std::reference_wrapper<const extensions> _extensions;

public:
    hint_endpoint_storage(const extensions& exts, seastar::scheduling_group sched_group) noexcept;
    
    hint_endpoint_storage(const hint_endpoint_storage&) = delete;
    hint_endpoint_storage& operator=(const hint_endpoint_storage&) = delete;
    
    hint_endpoint_storage(hint_endpoint_storage&&) noexcept = delete;
    hint_endpoint_storage& operator=(hint_endpoint_storage&&) noexcept = delete;
    
public:
    future<> start(const std::filesystem::path endpoint_hint_directory);
    future<> read_old_hints(const std::filesystem::path endpoint_hint_directory);

    future<> store_hint(schema_ptr, lw_shared_ptr<const frozen_mutation>) noexcept;
    future<> read_hints(hint_reader_type reader_functor);

private:
    future<> read_one_hint_file(const sstring& segment_name, hint_reader_type& reader_functor);
    future<> maybe_flush_hints();
    future<> flush_hints();
    future<> maybe_notify_waiters();
};

class hint_storage {
private:
};

} // namespace internal
} // namespace db::hints
