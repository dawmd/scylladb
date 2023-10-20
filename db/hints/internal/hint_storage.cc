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
#include <seastar/core/future.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>

// Boost features.
#include <boost/range/adaptors.hpp>

// Scylla includes.
#include "db/commitlog/commitlog_entry.hh"
#include "db/hints/internal/hint_logger.hh"
#include "seastar/core/semaphore.hh"
#include "seastar/util/later.hh"
#include "utils/disk-error-handler.hh"
#include "utils/lister.hh"
#include "utils/runtime.hh"

// STD.
#include <concepts>
#include <filesystem>
#include <functional>
#include <list>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>

namespace fs = std::filesystem;

namespace db::hints {
namespace internal {

namespace {

using segment_list = std::list<fs::path>;
// map: shard -> segments
using hints_ep_segments_map = std::unordered_map<unsigned, segment_list>;
// map: IP -> (map: shard -> segments)
using hints_segments_map = std::unordered_map<sstring, hints_ep_segments_map>;

future<> scan_shard_hint_directories(const fs::path& hint_directory,
        std::function<future<>(fs::path /* hint dir */, directory_entry, unsigned /* shard_id */)> func)
{
    return lister::scan_dir(hint_directory, lister::dir_entry_types::of<directory_entry_type::directory>(),
            [func = std::move(func)] (fs::path dir, directory_entry de) mutable {
        unsigned shard_id;

        try {
            shard_id = std::stoi(de.name);
        } catch (std::invalid_argument& ex) {
            manager_logger.debug("Ignore invalid directory {}", de.name);
            return make_ready_future<>();
        }

        return func(std::move(dir), std::move(de), shard_id);
    });
}

/// \brief Scan the given hint directory and build the map of all present hint segments.
///
/// Complexity: O(N+K), where N is a total number of present hint segments and
///                     K = <number of shards during the previous boot> * <number of endpoints
///                             for which hints where ever created>
///
/// \param hint_directory directory to scan
/// \return a map: ep -> map: shard -> segments (full paths)
future<hints_segments_map> get_current_hints_segments(const fs::path& hint_directory) {
    hints_segments_map current_hint_segments{};

    // Shard level.
    co_await scan_shard_hint_directories(hint_directory,
            [&current_hint_segments] (fs::path dir, directory_entry de, unsigned shard_id) {
        manager_logger.trace("shard_id = {}", shard_id);

        // IP level.
        return lister::scan_dir(dir / de.name, lister::dir_entry_types::of<directory_entry_type::directory>(),
                [&current_hint_segments, shard_id] (fs::path dir, directory_entry de) {
            manager_logger.trace("\tIP: {}", de.name);
            
            // Hint files.
            return lister::scan_dir(dir / de.name, lister::dir_entry_types::of<directory_entry_type::regular>(),
                    [&current_hint_segments, shard_id, ep = de.name] (fs::path dir, directory_entry de) {
                manager_logger.trace("\t\tfile: {}", de.name);

                current_hint_segments[ep][shard_id].emplace_back(dir / de.name);

                return make_ready_future<>();
            });
        });
    });

    co_return current_hint_segments;
}

/// \brief Rebalance hint segments for a given (destination) end point
///
/// This method is going to consume files from the \ref segments_to_move and distribute
/// them between the present shards (taking into an account the \ref ep_segments state - there may
/// be zero or more segments that belong to a particular shard in it) until we either achieve
/// the requested \ref segments_per_shard level on each shard or until we are out of files to move.
///
/// As a result (in addition to the actual state on the disk) both \ref ep_segments
/// and \ref segments_to_move are going to be modified.
///
/// Complexity: O(N), where N is a total number of present hint segments for
///             the \ref ep end point (as a destination).
///
/// \param ep destination end point ID (a string with its IP address)
/// \param segments_per_shard number of hint segments per-shard we want to achieve
/// \param hint_directory a root hint directory
/// \param ep_segments a map that was originally built by get_current_hints_segments() for this endpoint
/// \param segments_to_move a list of segments we are allowed to move
future<> rebalance_segments_for(const sstring& ep, size_t segments_per_shard,
        const fs::path& hint_directory, hints_ep_segments_map& ep_segments,
        segment_list& segments_to_move)
{
    manager_logger.trace("{}: segments_per_shard: {}, total number of segments to move: {}",
            ep, segments_per_shard, segments_to_move.size());

    // Sanity check.
    if (segments_to_move.empty() || !segments_per_shard) {
        co_return;
    }

    for (unsigned i = 0; i < smp::count && !segments_to_move.empty(); ++i) {
        const fs::path endpoint_dir_path = hint_directory / fmt::to_string(i) / ep;
        segment_list& current_shard_segments = ep_segments[i];

        // Make sure that the endpoint_dir_path exists. If not, create it.
        co_await io_check([name = endpoint_dir_path.c_str()] {
            return recursive_touch_directory(name);
        });

        while (current_shard_segments.size() < segments_per_shard && !segments_to_move.empty()) {
            auto seg_path_it = segments_to_move.begin();
            const fs::path seg_new_path = endpoint_dir_path / seg_path_it->filename();

            // Don't move the file to the same location. It's pointless.
            if (*seg_path_it != seg_new_path) {
                manager_logger.trace("going to move: {} -> {}", *seg_path_it, seg_new_path);
                co_await io_check(rename_file, seg_path_it->native(), seg_new_path.native());
            } else {
                manager_logger.trace("skipping: {}", *seg_path_it);
            }

            current_shard_segments.splice(current_shard_segments.end(), segments_to_move,
                    seg_path_it, std::next(seg_path_it));
        }
    }
}

/// \brief Rebalance all present hint segments.
///
/// The difference between the number of segments on any two shards will not be
/// greater than 1 after the rebalancing.
///
/// Complexity: O(N), where N is a total number of present hint segments.
///
/// \param hint_directory a root hint directory
/// \param segments_map a map that was built by get_current_hints_segments()
future<> rebalance_segments(const fs::path& hint_directory, hints_segments_map& segments_map) {
    // Count how many hint segments we have for each destination.
    std::unordered_map<sstring, size_t> per_ep_hints;

    for (const auto& [ep, ep_hint_segments] : segments_map) {
        per_ep_hints[ep] = boost::accumulate(ep_hint_segments
                | boost::adaptors::map_values
                | boost::adaptors::transformed(std::mem_fn(&segment_list::size)),
                size_t(0));
        manager_logger.trace("{}: total files: {}", ep, per_ep_hints[ep]);
    }

    // Create a map of lists of segments that we will move (for each destination endpoint):
    //   if a shard has segments, then we will NOT move q = int(N/S) segments out of them,
    //   where N is a total number of segments to the current destination
    //   and S is the current number of shards.
    std::unordered_map<sstring, segment_list> segments_to_move;

    for (auto& [ep, ep_segments] : segments_map) {
        const size_t q = per_ep_hints[ep] / smp::count;
        auto& current_segments_to_move = segments_to_move[ep];

        for (auto& [shard_id, shard_segments] : ep_segments) {
            // Move all segments from the shards that are no longer relevant
            // (re-sharding to the lower number of shards).
            if (shard_id >= smp::count) {
                current_segments_to_move.splice(current_segments_to_move.end(), shard_segments);
            } else if (shard_segments.size() > q) {
                current_segments_to_move.splice(current_segments_to_move.end(), shard_segments,
                        std::next(shard_segments.begin(), q), shard_segments.end());
            }
        }
    }

    // Since N (a total number of segments to a specific destination) may be not a multiple
    // of S (the current number of shards) we will distribute files in two passes:
    //    * if N = S * q + r, then
    //       * one pass for segments_per_shard = q
    //       * another one for segments_per_shard = q + 1.
    //
    // This way we will ensure as close to the perfect distribution as possible.
    //
    // Right till this point we haven't moved any segments. However we have created a logical
    // separation of segments into two groups:
    //    * Segments that are not going to be moved: segments in the segments_map.
    //    * Segments that are going to be moved: segments in the segments_to_move.
    //
    // rebalance_segments_for() is going to consume segments from segments_to_move
    // and move them to corresponding lists in the segments_map AND actually move segments
    // to the corresponding shard's sub-directory till the requested segments_per_shard level
    // is reached (see more details in the description of rebalance_segments_for()).
    for (const auto& [ep, N] : per_ep_hints) {
        const size_t q = N / smp::count;
        const size_t r = N - q * smp::count;
        auto& current_segments_to_move = segments_to_move[ep];
        auto& current_segments_map = segments_map[ep];

        if (q) {
            co_await rebalance_segments_for(ep, q, hint_directory, current_segments_map, current_segments_to_move);
        }

        if (r) {
            co_await rebalance_segments_for(ep, q + 1, hint_directory, current_segments_map, current_segments_to_move);
        }
    }
}

/// \brief Remove subdirectories of shards that are not relevant anymore (re-sharding to a lower number of shards case).
///
/// Complexity: O(S*E), where S is the number of shards during the previous boot and
///                           E is the number of endpoints for which hints were ever created.
///
/// \param hint_directory a root hint directory
future<> remove_irrelevant_shards_directories(const fs::path& hint_directory) {
    // Shard level.
    co_await scan_shard_hint_directories(hint_directory,
            [] (fs::path dir, directory_entry de, unsigned shard_id) -> future<> {
        if (shard_id >= smp::count) {
            // IP level.
            co_await lister::scan_dir(dir / de.name, lister::dir_entry_types::full(),
                    lister::show_hidden::yes, [] (fs::path dir, directory_entry de) {
                return io_check(remove_file, (dir / de.name).native());
            });
            
            co_await io_check(remove_file, (dir / de.name).native());
        }
    });
}

} // anonymous namespace

future<> rebalance_hints(fs::path hint_directory) {
    // Scan currently present hint segments.
    hints_segments_map current_hints_segments = co_await get_current_hints_segments(hint_directory);

    // Move segments to achieve an even distribution of files among all present shards.
    co_await rebalance_segments(hint_directory, current_hints_segments);

    // Remove the directories of shards that are not present anymore.
    // They should not have any segments by now.
    co_await remove_irrelevant_shards_directories(hint_directory);
}


/////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////

// The reason why we use std::string rather than seastar::sstring is simple -- commitlog uses it.
static const std::string HINT_FILENAME_PREFIX = {"HintsLog" + commitlog::descriptor::SEPARATOR};
static constexpr std::chrono::seconds HINT_FILE_WRITE_TIMEOUT = std::chrono::seconds(2);

hint_endpoint_storage::hint_endpoint_storage(const extensions& exts, seastar::scheduling_group sched_group) noexcept
    : _write_rp(this_shard_id(), std::chrono::duration_cast<std::chrono::milliseconds>(
            runtime::get_boot_time().time_since_epoch()).count())
    , _sched_group(sched_group)
    , _flush_timer([this] {
        // Unfortunately, the callback cannot return a future, so we need to discard it here.
        (void) maybe_flush_hints();
    })
    , _extensions(exts)
{}

future<> hint_endpoint_storage::start(const fs::path endpoint_hint_directory) {
    co_await io_check([name = std::string_view{endpoint_hint_directory.native()}] {
        return recursive_touch_directory(name);
    });

    commitlog::config cfg;

    cfg.commit_log_location = endpoint_hint_directory.native();
    cfg.commitlog_segment_size_in_mb = HINT_SEGMENT_SIZE_IN_MB;
    cfg.commitlog_total_space_in_mb = MAX_HINT_SIZE_PER_ENDPOINT_IN_MB;
    cfg.fname_prefix = HINT_FILENAME_PREFIX;
    cfg.extensions = std::addressof(_extensions.get());

    // HH leaves segments on disk after commitlog shutdown, and later reads
    // them when commitlog is re-created. This is expected to happen regularly
    // during standard HH workload, so no need to print a warning about it.
    cfg.warn_about_segments_left_on_disk_after_shutdown = false;
    // Allow going over the configured size limit of the commitlog
    // (resource_manager::max_hints_per_ep_size_mb). The commitlog will
    // be more conservative with its disk usage when going over the limit.
    // On the other hand, HH counts used space using the space_watchdog
    // in resource_manager, so its redundant for the commitlog to apply
    // a hard limit.
    cfg.allow_going_over_size_limit = true;
    // The API for waiting for hint replay relies on replay positions
    // monotonically increasing. When there are no segments on disk,
    // by default the commitlog will calculate the first segment ID
    // based on the boot time. This may cause the following sequence
    // of events to occur:
    //
    // 1. Node starts with empty hints queue
    // 2. Some hints are written and some segments are created
    // 3. All hints are replayed
    // 4. Hint sync point is created
    // 5. Commitlog instance gets re-created and resets it segment ID counter
    // 6. New hint segment has the first ID as the first (deleted by now) segment
    // 7. Waiting for the sync point commences but resolves immediately
    //    before new hints are replayed - since point 5., `_last_written_rp`
    //    and `_sent_upper_bound_rp` are not updated because RPs of new
    //    hints are much lower than both of those marks.
    //
    // In order to prevent this situation, we override the base segment ID
    // of the newly created commitlog instance - it should start with an ID
    // which is larger than the segment ID of the RP of the last written hint.
    cfg.base_segment_id = _write_rp.base_id();

    _commitlog = std::make_unique<commitlog>(co_await commitlog::create_commitlog(std::move(cfg)));
}

future<> hint_endpoint_storage::read_old_hints(const fs::path endpoint_hint_directory) {
    auto segment_vector = co_await _commitlog->get_segments_to_replay();
    if (segment_vector.empty()) [[unlikely]] {
        co_return;
    }

    std::vector<std::pair<segment_id_type, sstring>> segments{};
    segments.reserve(segment_vector.size());

    const auto my_shard_id = this_shard_id();
    for (auto& segment : segment_vector) {
        commitlog::descriptor desc{segment, HINT_FILENAME_PREFIX};
        if (replay_position{desc}.shard_id() == my_shard_id) {
            segments.emplace_back(desc.id, std::move(segment));
        } else {
            _foreign_segment_list.emplace_back(std::move(segment));
        }
    }

    // Sort segments that belonged to this shard based on their segment IDs,
    // which should correspond to the chronological order.
    std::sort(segments.begin(), segments.end());
    for (auto& [_, segment] : segments) {
        _segment_list.emplace_back(std::move(segment));
    }
}

future<> hint_endpoint_storage::store_hint(schema_ptr s, lw_shared_ptr<const frozen_mutation> fm) noexcept
{
    const auto mutex_lock = co_await seastar::get_units(_file_update_mutex, 1);
    commitlog_entry_writer cew{s, *fm, commitlog::force_sync::no};

    rp_handle rph = co_await _commitlog->add_entry(s->id(), cew, timeout_clock_type::now() + HINT_FILE_WRITE_TIMEOUT);
    _write_rp = std::max(_write_rp, rph.release());
}

struct hint_file_reading_stopped {};

future<> hint_endpoint_storage::read_hints(hint_reader_type reader_function) {
    if (_foreign_segment_list.empty() && _segment_list.empty()) {
        co_return;
    }

    try {
        while (true) {
            auto& slist = !_foreign_segment_list.empty()
                    ? _foreign_segment_list
                    : _segment_list;
            
            co_await read_one_hint_file(slist.front(), reader_function);
            
            slist.pop_front();
            _read_rp = 0;
        }
    } catch (const hint_file_reading_stopped&) {/* ignore */}
}

future<> hint_endpoint_storage::read_one_hint_file(const sstring& segment_name, hint_reader_type& reader) {
    bool modify_rp = true;
    
    co_await commitlog::read_log_file(segment_name, HINT_FILENAME_PREFIX,
            coroutine::lambda([&] (commitlog::buffer_and_replay_position bufrp) mutable -> future<> {
        auto& buffer = bufrp.buffer;
        auto& rp = bufrp.position;

        const auto result = co_await reader(hint_entry_reader{buffer}, buffer.size_bytes());

        switch (result) {
        case hint_reader_result::CONTINUE_FORGET:
            if (modify_rp) {
                _read_rp = std::max(_read_rp, rp);
            }
            break;
        case hint_reader_result::CONTINUE_KEEP:
            modify_rp = false;
            break;
        case hint_reader_result::STOP_FORGET:
            [[fallthrough]];
        case hint_reader_result::STOP_KEEP:
            throw hint_file_reading_stopped{};
        }

        co_await maybe_notify_waiters();
    }));
}

future<> hint_endpoint_storage::maybe_flush_hints() {
    // For now, we don't really check anything. It might even be unnecessary in general.
    co_await flush_hints();
}

future<> hint_endpoint_storage::flush_hints() {
    co_await _commitlog->sync_all_segments();
    _flush_timer.rearm(timeout_clock_type::now() + HINT_FILE_WRITE_TIMEOUT);
}

future<> hint_endpoint_storage::maybe_notify_waiters() {
    // TO BE IMPLEMENTED
    co_return;
}

} // namespace internal
} // namespace db::hints
