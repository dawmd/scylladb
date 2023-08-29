/*
 * Modified by ScyllaDB
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

// Seastar features.
#include <seastar/core/file.hh>
#include <seastar/core/file-types.hh>
#include <seastar/core/future.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/with_scheduling_group.hh>

// Scylla includes.
#include "db/commitlog/commitlog.hh"
#include "db/commitlog/commitlog_entry.hh"
#include "db/hints/internal/common.hh"
#include "utils/lister.hh"
#include "utils/loading_shared_values.hh"

// STD.
#include <chrono>
#include <concepts>
#include <filesystem>
#include <optional>
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


/// This class is responsible for managing hints corresponding to a specific host and local shard.
/// Its main functionality is to store and read hints from the disk.
///
/// Its actions can be assigned to a specific scheduling group.
class host_hint_storage {
private:
    // Path to the directory corresponding to this host on a specific shard.
    // In the usual configuration, it should be "{SCYLLA_WORKDIR}/{HINT_DIR}/<shard_id>/<host_id>".
    std::filesystem::path _host_dir_path;
    // Scheduling group that I/O operations performed by this object should belong to.
    // If equal to `std::nullopt`, no scheduling group is set.
    std::optional<seastar::scheduling_group> _maybe_sched_group;

public:
    host_hint_storage(const std::filesystem::path& shard_hint_storage_path, host_id_type host_id,
            std::optional<seastar::scheduling_group> maybe_sched_group);
    ~host_hint_storage() noexcept = default;

public:
    const std::filesystem::path& path() const noexcept {
        return _host_dir_path;
    }

    std::optional<seastar::scheduling_group> scheduling_group() const noexcept {
        return _maybe_sched_group;
    }

    template <typename Func>
        requires std::invocable<Func, seastar::directory_entry>
    seastar::future<> for_each_hint_file(Func&& func) {
        // We mark this lambda as mutable because `operator()(seastar::directory_entry)` of
        // the passed functor can be marked as const or there could be two overloads:
        // one for when it's const-qualified, and another for when it's not.
        // We want to use the right one.
        auto lambda = [func = std::forward<Func>(func)] (auto&& /* ignored */,
                seastar::directory_entry de) mutable {
            return func(std::move(de));
        };

        co_await maybe_invoke_with_scheduling_group(lister::scan_dir, _host_dir_path,
                lister::dir_entry_types::of<seastar::directory_entry_type::regular>(),
                // std::ref to leverage the small object optimization of std::function
                // that all major compilers implement. If the passed functor's size is
                // big, that would lead to unnecessary allocations that we can avoid.
                // This local lambda will live until this function returns because of
                // lifetime extension related to how coroutines are implemented.
                std::ref(lambda));
    }

private:
    template <typename Func, typename... Args>
    decltype(auto) maybe_invoke_with_scheduling_group(Func&& func, Args&&... args) {
        return _maybe_sched_group.has_value()
                ? seastar::with_scheduling_group(_maybe_sched_group.value(), std::forward<Func>(func),
                        std::forward<Args>(args)...)
                : std::forward<Func>(func)(std::forward<Args>(args)...);
    }
};


/// This class is responsible for managing the directory corresponding to a specific shard.
/// It allows for browsing which hosts are currently managed (i.e. we store some hints to them).
///
/// The class does NOT implement any mechanism related to storing or reading hints.
/// That's a responsibility of @ref host_hint_storage objects.
/// This class functions as an interface for creating those, though.
///
/// Its actions can be assigned to a specific scheduling group.
class shard_hint_storage {
private:
    // Path to the directory corresponding to this shard.
    // In the usual configuration, it should be "{SCYLLA_WORKDIR}/{HINT_DIR}/<shard_id>/".
    std::filesystem::path _dir_path;
    // Scheduling group that I/O operations performed by this object should belong to.
    // If equal to `std::nullopt`, no scheduling group is set.
    std::optional<seastar::scheduling_group> _maybe_sched_group = std::nullopt;

public:
    shard_hint_storage(const std::filesystem::path& hint_dir_path);
    shard_hint_storage(const std::filesystem::path& hint_dir_path, seastar::scheduling_group sched_group);

    // TODO?
    ~shard_hint_storage() noexcept = default;

public:
    const std::filesystem::path& path() const noexcept {
        return _dir_path;
    }

    std::optional<seastar::scheduling_group> scheduling_group() const noexcept {
        return _maybe_sched_group;
    }

    template <typename Func>
        requires std::invocable<Func, host_id_type>
    seastar::future<> for_each_host_dir(Func&& func) {
        // We mark this lambda as mutable because `operator()(host_id_type)` of the passed functor
        // can be marked as const or there could be two overloads: one for when it's const-qualified,
        // and another for when it's not. We want to use the right one.
        auto lambda = [func = std::forward<Func>(func)] (auto&& /* ignored */,
                seastar::directory_entry de) mutable {
            const auto host_id = host_id_type{de.name};
            return func(host_id);
        };

        co_await maybe_invoke_with_scheduling_group(lister::scan_dir, _dir_path,
                lister::dir_entry_types::of<seastar::directory_entry_type::directory>(),
                // std::ref to leverage the small object optimization of std::function
                // that all major compilers implement. If the passed functor's size is
                // big, that would lead to unnecessary allocations that we can avoid.
                // This local lambda will live until this function returns because of
                // lifetime extension related to how coroutines are implemented.
                std::ref(lambda));
    }

    host_hint_storage get_host_hint_storage_for(host_id_type host_id) const;

private:
    template <typename Func, typename... Args>
    decltype(auto) maybe_invoke_with_scheduling_group(Func&& func, Args&&... args) {
        return _maybe_sched_group.has_value()
                ? seastar::with_scheduling_group(_maybe_sched_group.value(), std::forward<Func>(func),
                        std::forward<Args>(args)...)
                : std::forward<Func>(func)(std::forward<Args>(args)...);
    }
};


/// \brief Rebalance hints segments among ALL present shards.
///
/// The difference between the number of segments on every two shard will be not greater
/// than 1 after the rebalancing.
///
/// Removes the sub-directories of \ref hints_directory that correspond to shards
/// that are not relevant any more (re-sharding to a lower shards number case).
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
