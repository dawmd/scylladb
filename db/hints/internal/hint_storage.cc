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
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/print.hh> // For seastar::format
#include <seastar/core/scheduling.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/thread.hh>

// Boost features.
#include <boost/range/adaptors.hpp>

// Scylla includes.
#include "db/commitlog/commitlog_entry.hh"
#include "db/commitlog/replay_position.hh"
#include "db/extensions.hh"
#include "db/hints/internal/hint_logger.hh"
#include "utils/disk-error-handler.hh"
#include "utils/lister.hh"
#include "utils/runtime.hh"

// STD.
#include <cassert>
#include <chrono>
#include <exception>
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
    for (const auto& [ip, host_segment_map] : segments_map) {
        per_ep_hints[ip] = boost::accumulate(
                host_segment_map |
                boost::adaptors::map_values |
                boost::adaptors::transformed(std::mem_fn(&std::list<fs::path>::size)),
                size_t(0));
        manager_logger.trace("{}: total files: {}", ip, per_ep_hints[ip]);
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
    for (const auto& [ep, N] : per_ep_hints) {
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


/// This class functions as a proxy between @ref host_hint_storage and @ref commitlog.
///
/// It introduces additional mechanisms like buffering. Commitlog wasn't written with
/// runtime in mind (i.e. it didn't assume that the user may want to both read from
/// and write to it at runtime), which results in several difficulties related to
/// the semantics of Hinted Handoff. This wrapper is supposed to encapsulate necessary
/// guarantees as well as simply make our life easier.
class host_hint_storage::hint_commitlog final {
private:
    using clock_type = seastar::lowres_clock;
    static_assert(noexcept(clock_type::now()), "clock_type::now() must be noexcept");

    using time_point_type = typename clock_type::time_point;
    using duration_type = typename clock_type::duration;

    // We want to buffer active (i.e. still relevant) segments' names. Commitlog requires providing
    // the name of the file you want to read. Although its API provides a way to obtain that
    // information, it returns the names of ALL segments. We want to avoid that unnecessary
    // additional work, so we'll buffer the names on your own.
    using segment_list = std::list<seastar::sstring>;

private:
    std::reference_wrapper<const host_hint_storage> _parent;
    std::reference_wrapper<const extensions> _extensions;

    // Replay position corresponding to the last successful write to the commitlog.
    // It is used when recreating a commitlog instance. See the method: `create_commitlog`.
    replay_position _last_written_rp;
    // List of the names of active segments.
    segment_list _segment_list;
    // With the current implementation of commitlog, we're forced to create, delete and create
    // commitlog instances to ensure that we can also read hints at runtime. It's related to
    // buffering in commitlog. After flushing hints, this pointer will again be equal to `nullptr`.
    std::unique_ptr<commitlog> _commitlog_ptr = nullptr;

    seastar::semaphore _mutex{1};
    time_point_type _next_flush_time_point;

public:
    hint_commitlog(const host_hint_storage& parent, const extensions& exts) noexcept
        : _parent{parent}
        , _extensions{exts}
        // TODO: Should this logic be deduplicated with what is in the commitlog?
        , _last_written_rp{seastar::this_shard_id(),
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    runtime::get_boot_time().time_since_epoch()).count()}
    {}
    
    ~hint_commitlog() noexcept = default;

public:
    template <typename... Args>
    seastar::future<> add_entry(Args&&... args) {
        commitlog& cl = co_await get_or_create_commitlog();
        rp_handle rph = co_await cl.add_entry(std::forward<Args>(args)...);
        _last_written_rp = std::max(_last_written_rp, rph.release());
    }

    const extensions& extensions_used() const noexcept {
        return _extensions;
    }

private:
    seastar::future<> flush_maybe() {
        const auto current_time = clock_type::now();
        if (current_time > _next_flush_time_point) {
            try {
                co_await flush_hints();
                _next_flush_time_point = current_time + 
            }
        }
    }

    seastar::future<> flush_hints() {

    }

    seastar::future<std::reference_wrapper<commitlog>> get_or_create_commitlog() {
        // To avoid deadlock in case multiple fibers simultaneously realise
        // that there is no commitlog instance at the moment.
        const auto lock = seastar::get_units(_mutex, 1);
        // Unlikely because we flush relatively rarely.
        if (!_commitlog_ptr) [[unlikely]] {
            co_await create_commitlog();
        }
        co_return *_commitlog_ptr;
    }

    seastar::future<> create_commitlog() {
        assert(!_commitlog_ptr);

        using commitlog_config = typename commitlog::config;
        commitlog_config cfg;

        cfg.sched_group = _parent.get()._maybe_sched_group.value();
        cfg.commit_log_location = _parent.get()._host_dir_path.c_str();
        cfg.commitlog_segment_size_in_mb = HINT_SEGMENT_SIZE_IN_MB;
        cfg.commitlog_total_space_in_mb = MAX_HINTS_PER_HOST_SIZE_MB;
        cfg.fname_prefix = HINT_FILENAME_PREFIX;
        cfg.extensions = std::addressof(_extensions.get());

        // HH leaves segments on disk after commitlog shutdown, and later reads
        // them when commitlog is re-created. This is expected to happen regularly
        // during standard HH workload, so no need to print a warning about it.
        cfg.warn_about_segments_left_on_disk_after_shutdown = false;
        // Allow going over the configured size limit of the commitlog
        // (MAX_HINTS_PER_HOST_SIZE_MB). The commitlog will be more conservative
        // with its disk usage when going over the limit.
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
        cfg.base_segment_id = _last_written_rp.base_id();

        _commitlog_ptr = std::make_unique<commitlog>(co_await commitlog::create_commitlog(std::move(cfg)));
    }
};


////////////////////////////////////////////////////////
////////////////////////////////////////////////////////
////////////////////////////////////////////////////////
////////////////////////////////////////////////////////


namespace {

/// \brief Get the last modification time stamp for a given file.
/// \param fname File name
/// \return The last modification time stamp for \param fname.
seastar::future<::timespec> get_last_file_modification(const std::string_view fname) {
    seastar::file f = co_await seastar::open_file_dma(fname, seastar::open_flags::ro);
    const auto st = co_await f.stat();
    co_return st.st_mtim;
}

} // anonymous namespace

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

seastar::future<> host_hint_storage::store_hint(schema_ptr s,
        seastar::lw_shared_ptr<const frozen_mutation> fm, tracing::trace_state_ptr tr_state)
{
    const auto gate_holder = seastar::gate::holder{_store_gate};

    // `*this` is implicitly captured by reference even if the default capture is `=`.
    co_await seastar::with_shared(_shared_mutex,
            seastar::coroutine::lambda([=] () mutable -> seastar::future<> {
        try {
            commitlog& cl = co_await get_or_load();
            commitlog_entry_writer cew{s, *fm, commitlog::force_sync::no};

            rp_handle rph = co_await cl.add_entry(s->id(), cew,
                    timeout_clock::now() + HINT_FILE_WRITE_TIMEOUT);
            const auto rp = rph.release();
            if (_last_written_rp < rp) {
                _last_written_rp = rp;
                // TODO: Change this logging.
                manager_logger.debug("Updated last written position of {} to {}", _host_dir_path, rp);
            }

            // TODO: Change this logging.
            manager_logger.trace("Hint to {} has been stored", _host_dir_path);
            tracing::trace(tr_state, "Hint to {} has been stored", _host_dir_path);
        } catch (...) {
            std::exception_ptr eptr = std::current_exception();
            manager_logger.trace("Failed to store a hint to {}: {}", _host_dir_path, eptr);
            tracing::trace(tr_state, "Failed to store a hint to {}: {}", _host_dir_path, eptr);
        }
    }));
}

seastar::future<> host_hint_storage::read_hints(hint_reader_type callback) {
    const auto last_file_mod = co_await get_last_file_modification(_)
}

seastar::future<std::reference_wrapper<commitlog>> host_hint_storage::get_or_load() {
    if (!_commitlog_ptr) {
        // This causes a deadlock...
        co_await seastar::with_lock(_shared_mutex, [this] {
            return create_commitlog(...).then([this] (commitlog cl) {
                _commitlog_ptr = std::make_unique<commitlog>(std::move(cl));
            });
        });
    }

    co_return *_commitlog_ptr;
}

// Assumes the directory has already been created.
seastar::future<commitlog> host_hint_storage::create_commitlog(const extensions& extens,
        segment_id_type base_segment_id)
{
    using commitlog_config = typename commitlog::config;
    commitlog_config cfg;

    if (_maybe_sched_group) {
        cfg.sched_group = _maybe_sched_group.value();
    }
    cfg.commit_log_location = _host_dir_path.c_str();
    cfg.commitlog_segment_size_in_mb = HINT_SEGMENT_SIZE_IN_MB;
    cfg.commitlog_total_space_in_mb = MAX_HINTS_PER_HOST_SIZE_MB;
    cfg.fname_prefix = HINT_FILENAME_PREFIX;
    cfg.extensions = std::addressof(extens);

    // HH leaves segments on disk after commitlog shutdown, and later reads
    // them when commitlog is re-created. This is expected to happen regularly
    // during standard HH workload, so no need to print a warning about it.
    cfg.warn_about_segments_left_on_disk_after_shutdown = false;
    // Allow going over the configured size limit of the commitlog
    // (MAX_HINTS_PER_HOST_SIZE_MB). The commitlog will be more conservative
    // with its disk usage when going over the limit.
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
    cfg.base_segment_id = base_segment_id;

    co_return co_await commitlog::create_commitlog(std::move(cfg));
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
