/*
 * Modified by ScyllaDB
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "db/hints/internal/hint_storage.hh"

// Seastar features.
#include <seastar/core/coroutine.hh>
#include <seastar/core/file.hh>
#include <seastar/core/file-types.hh>
#include <seastar/core/print.hh> // For seastar::format
#include <seastar/core/seastar.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread.hh>

// Boost features.
#include <boost/range/adaptors.hpp>

// Scylla includes.
#include "db/hints/internal/hint_logger.hh"
#include "seastar/core/future.hh"
#include "seastar/core/scheduling.hh"
#include "utils/disk-error-handler.hh"
#include "utils/lister.hh"

// STD.
#include <filesystem>
#include <functional>
#include <list>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>

namespace fs = std::filesystem;

namespace db::hints {
namespace internal {

namespace {

// map: shard -> segments
using hint_host_segments_map = std::unordered_map<seastar::shard_id, std::list<fs::path>>;
// map: IP -> (map: shard -> segments)
using hint_segments_map = std::unordered_map<seastar::sstring, hint_host_segments_map>;

template <typename Func>
    requires std::invocable<Func, fs::path, seastar::directory_entry, seastar::shard_id>
seastar::future<> scan_for_hints_dirs(const std::string_view hints_directory, Func&& func) {
    return lister::scan_dir(hints_directory, lister::dir_entry_types::of<seastar::directory_entry_type::directory>(),
            [func = std::forward<Func>(func)] (fs::path dir, seastar::directory_entry de) mutable {
        seastar::shard_id shard_id;

        try {
            shard_id = std::stoi(de.name.c_str());
        } catch (std::invalid_argument& ex) {
            manager_logger.debug("Ignore invalid directory {}", de.name);
            return seastar::make_ready_future<>();
        }
        
        return func(std::move(dir), std::move(de), shard_id);
    });
}

/// \brief Scan the given hints directory and build the map of all present hints segments.
///
/// Complexity: O(N+K), where N is a total number of present hints' segments and
///                           K = <number of shards during the previous boot> * <number of end points for which hints where ever created>
///
/// \param hint_directory directory to scan
/// \return a map: ep -> map: shard -> segments (full paths)
seastar::future<hint_segments_map> get_current_hints_segments(const std::string_view hint_directory) {
    hint_segments_map current_hints_segments;

    // Shard level.
    co_await scan_for_hints_dirs(hint_directory, [&current_hints_segments] (fs::path dir,
            seastar::directory_entry de, seastar::shard_id shard_id) {
        manager_logger.trace("shard_id = {}", shard_id);

        // IP level.
        return lister::scan_dir(dir / de.name, lister::dir_entry_types::of<seastar::directory_entry_type::directory>(),
                [&current_hints_segments, shard_id] (fs::path dir, seastar::directory_entry de) {
            manager_logger.trace("\tIP: {}", de.name);

            // Hint files.
            return lister::scan_dir(dir / de.name, lister::dir_entry_types::of<seastar::directory_entry_type::regular>(),
                    [&current_hints_segments, shard_id, ep_addr = de.name] (fs::path dir,
                            seastar::directory_entry de) {
                manager_logger.trace("\t\tfile: {}", de.name);
                current_hints_segments[ep_addr][shard_id].emplace_back(dir / de.name);
                return seastar::make_ready_future<>();
            });
        });
    });

    co_return current_hints_segments;
}

/// \brief Rebalance hints segments for a given (destination) end point
///
/// This method is going to consume files from the \ref segments_to_move and distribute them between the present
/// shards (taking into an account the \ref host_segments state - there may be zero or more segments that belong to a
/// particular shard in it) until we either achieve the requested \ref segments_per_shard level on each shard
/// or until we are out of files to move.
///
/// As a result (in addition to the actual state on the disk) both \ref host_segments and \ref segments_to_move are going
/// to be modified.
///
/// Complexity: O(N), where N is a total number of present hints' segments for the \ref ep end point (as a destination).
///
/// \param ep destination end point ID (a string with its IP address)
/// \param segments_per_shard number of hints segments per-shard we want to achieve
/// \param hint_directory a root hints directory
/// \param host_segments a map that was originally built by get_current_hint_segments() for this end point
/// \param segments_to_move a list of segments we are allowed to move
seastar::future<> rebalance_segments_for(const std::string_view ep, size_t segments_per_shard,
        const std::string_view hint_directory, hint_host_segments_map& host_segments,
        std::list<fs::path>& segments_to_move)
{
    manager_logger.trace("{}: segments_per_shard: {}, total number of segments to move: {}",
            ep, segments_per_shard, segments_to_move.size());

    // sanity check
    if (segments_to_move.empty() || !segments_per_shard) {
        co_return;
    }

    const fs::path hint_directory_path{hint_directory};

    for (seastar::shard_id i = 0; i < smp::count && !segments_to_move.empty(); ++i) {
        fs::path shard_path_dir{hint_directory_path / seastar::format("{:d}", i) / ep};
        std::list<fs::path>& current_shard_segments = host_segments[i];

        // Make sure that the shard_path_dir exists and if not - create it
        co_await io_check([name = shard_path_dir.c_str()] {
            return seastar::recursive_touch_directory(name);
        });

        while (current_shard_segments.size() < segments_per_shard && !segments_to_move.empty()) {
            auto seg_path_it = segments_to_move.begin();
            fs::path new_path{shard_path_dir / seg_path_it->filename()};

            // Don't move the file to the same location - it's pointless.
            if (*seg_path_it != new_path) {
                manager_logger.trace("going to move: {} -> {}", *seg_path_it, new_path);
                co_await io_check(seastar::rename_file, seg_path_it->native(), new_path.native());
            } else {
                manager_logger.trace("skipping: {}", *seg_path_it);
            }
            current_shard_segments.splice(current_shard_segments.end(), segments_to_move,
                    seg_path_it, std::next(seg_path_it));
        }
    }
}

/// \brief Rebalance all present hints segments.
///
/// The difference between the number of segments on every two shard will be not greater than 1 after the
/// rebalancing.
///
/// Complexity: O(N), where N is a total number of present hints' segments.
///
/// \param hint_directory a root hints directory
/// \param segments_map a map that was built by get_current_hint_segments()
seastar::future<> rebalance_segments(const std::string_view hint_directory, hint_segments_map& segments_map) {
    // Count how many hints segments to each destination we have.
    std::unordered_map<seastar::sstring, size_t> per_ep_hints;
    for (auto& ep_info : segments_map) {
        per_ep_hints[ep_info.first] = boost::accumulate(
                ep_info.second |
                boost::adaptors::map_values |
                boost::adaptors::transformed(std::mem_fn(&std::list<fs::path>::size)),
                size_t(0));
        manager_logger.trace("{}: total files: {}", ep_info.first, per_ep_hints[ep_info.first]);
    }

    // Create a map of lists of segments that we will move (for each destination end point):
    // if a shard has segments then we will NOT move q = int(N/S) segments out of them,
    // where N is a total number of segments to the current destination
    // and S is a current number of shards.
    std::unordered_map<seastar::sstring, std::list<fs::path>> segments_to_move;
    for (auto& [ep, ep_segments] : segments_map) {
        size_t q = per_ep_hints[ep] / smp::count;
        auto& current_segments_to_move = segments_to_move[ep];

        for (auto& [shard_id, shard_segments] : ep_segments) {
            // Move all segments from the shards that are no longer relevant (re-sharding to the lower number of shards)
            if (shard_id >= smp::count) {
                current_segments_to_move.splice(current_segments_to_move.end(), shard_segments);
            } else if (shard_segments.size() > q) {
                current_segments_to_move.splice(current_segments_to_move.end(), shard_segments,
                        std::next(shard_segments.begin(), q), shard_segments.end());
            }
        }
    }

    // Since N (a total number of segments to a specific destination) may be not a multiple of S
    // (a current number of shards) we will distribute files in two passes:
    //    * if N = S * q + r, then
    //       * one pass for segments_per_shard = q
    //       * another one for segments_per_shard = q + 1.
    //
    // This way we will ensure as close to the perfect distribution as possible.
    //
    // Right till this point we haven't moved any segments. However we have created
    // a logical separation of segments into two groups:
    //    * Segments that are not going to be moved: segments in the segments_map.
    //    * Segments that are going to be moved: segments in the segments_to_move.
    //
    // rebalance_segments_for() is going to consume segments from segments_to_move and move
    // them to corresponding lists in the segments_map AND actually move segments to
    // the corresponding shard's sub-directory till the requested segments_per_shard level
    // is reached (see more details in the description of rebalance_segments_for()).
    for (auto& [ep, N] : per_ep_hints) {
        size_t q = N / smp::count;
        size_t r = N - q * smp::count;
        auto& current_segments_to_move = segments_to_move[ep];
        auto& current_segments_map = segments_map[ep];

        if (q) {
            co_await rebalance_segments_for(ep, q, hint_directory,
                    current_segments_map, current_segments_to_move);
        }

        if (r) {
            co_await rebalance_segments_for(ep, q + 1, hint_directory,
                    current_segments_map, current_segments_to_move);
        }
    }
}

/// \brief Remove sub-directories of shards that are not relevant any more (re-sharding to a lower number of shards case).
///
/// Complexity: O(S*E), where S is a number of shards during the previous boot and
///                           E is a number of end points for which hints where ever created.
///
/// \param hint_directory a root hints directory
seastar::future<> remove_irrelevant_shards_directories(const std::string_view hint_directory) {
    // Shard level.
    return scan_for_hints_dirs(hint_directory, [] (fs::path dir, seastar::directory_entry de,
            seastar::shard_id shard_id) -> seastar::future<> {
        if (shard_id >= smp::count) {
            // IP level.
            co_await lister::scan_dir(dir / de.name, lister::dir_entry_types::full(),
                    lister::show_hidden::yes, [] (fs::path dir, seastar::directory_entry de) {
                return io_check(seastar::remove_file, (dir / de.name).native());
            });
            co_await io_check(seastar::remove_file, (dir / de.name).native());
        }
    });
}

} // anonymous namespace

seastar::future<> rebalance_hints(seastar::sstring hints_directory) {
    // Scan currently present hints segments.
    hint_segments_map current_hints_segments = co_await get_current_hints_segments(hints_directory);

    // Move segments to achieve an even distribution of files among all present shards.
    co_await rebalance_segments(hints_directory, current_hints_segments);

    // Remove the directories of shards that are not present anymore - they should not have any segments by now
    co_await remove_irrelevant_shards_directories(hints_directory);
}


////////////////////////////////////////////////////////
////////////////////////////////////////////////////////
////////////////////////////////////////////////////////
////////////////////////////////////////////////////////


host_hint_storage::host_hint_storage(const std::filesystem::path& shard_hint_storage_path,
        host_id_type host_id, std::optional<seastar::scheduling_group> maybe_sched_group)
    : _host_dir_path{shard_hint_storage_path / host_id.to_sstring()}
    , _maybe_sched_group{maybe_sched_group}
{}

seastar::future<> host_hint_storage::ensure_directory_existence() const {
    return io_check([name = _host_dir_path.c_str()] {
        return seastar::recursive_touch_directory(name);
    });
}


////////////////////////////////////////////////////////
////////////////////////////////////////////////////////


shard_hint_storage::shard_hint_storage(const fs::path& hint_dir_path)
    : _dir_path{hint_dir_path / seastar::format("{:d}", seastar::this_shard_id())}
{}

shard_hint_storage::shard_hint_storage(const fs::path& hint_dir_path, seastar::scheduling_group sched_group)
    : _dir_path{hint_dir_path / seastar::format("{:d}", seastar::this_shard_id())}
    , _maybe_sched_group{sched_group}
{}

host_hint_storage shard_hint_storage::get_host_hint_storage_for(host_id_type host_id) const {
    return host_hint_storage{_dir_path, host_id, _maybe_sched_group};
}


} // namespace internal
} // namespace db::hints
