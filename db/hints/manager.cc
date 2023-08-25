/*
 * Modified by ScyllaDB
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "db/hints/manager.hh"

// Seastar features.
#include <exception>
#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/future.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/parallel_for_each.hh>

// Boost features.
#include <boost/range/adaptors.hpp>

// Scylla includes.
#include "db/hints/internal/hint_logger.hh"
#include "db/hints/internal/hint_storage.hh"
#include "db/extensions.hh"
#include "db/timeout_clock.hh"
#include "gms/gossiper.hh"
#include "gms/versioned_value.hh"
#include "locator/abstract_replication_strategy.hh"
#include "mutation/mutation_partition_view.hh"
#include "replica/database.hh"
#include "seastar/core/file-types.hh"
#include "service/storage_proxy.hh"
#include "utils/directories.hh"
#include "utils/disk-error-handler.hh"
#include "utils/div_ceil.hh"
#include "utils/error_injection.hh"
#include "utils/lister.hh"
#include "utils/runtime.hh"
#include "converting_mutation_partition_applier.hh"
#include "seastarx.hh"
#include "service_permit.hh"

// STD.
#include <algorithm>
#include <filesystem>
#include <ranges>
#include <span>

using namespace std::literals::chrono_literals;
using namespace db::hints::internal;

namespace fs = std::filesystem;

namespace db::hints {

std::chrono::seconds manager::hints_flush_period = std::chrono::seconds(10);

manager::manager(seastar::sstring hints_directory, host_filter filter, int64_t max_hint_window_ms,
        resource_manager& res_manager, seastar::distributed<replica::database>& db)
    : _hints_dir{fs::path{hints_directory} / seastar::format("{:d}", seastar::this_shard_id())}
    , _host_filter{std::move(filter)}
    , _max_hint_window_us{max_hint_window_ms * 1000}
    , _local_db{db.local()}
    , _resource_manager{res_manager}
{
    if (utils::get_local_injector().enter("decrease_hints_flush_period")) {
        hints_flush_period = std::chrono::seconds{1};
    }
}

void manager::register_metrics(const seastar::sstring& group_name) {
    namespace sm = seastar::metrics;

    _metrics.add_group(group_name, {
        sm::make_gauge("size_of_hints_in_progress", _stats.size_of_hints_in_progress,
                        sm::description("Size of hinted mutations that are scheduled to be written.")),

        sm::make_counter("written", _stats.written,
                        sm::description("Number of successfully written hints.")),

        sm::make_counter("errors", _stats.errors,
                        sm::description("Number of errors during hints writes.")),

        sm::make_counter("dropped", _stats.dropped,
                        sm::description("Number of dropped hints.")),

        sm::make_counter("sent", _stats.sent,
                        sm::description("Number of sent hints.")),

        sm::make_counter("discarded", _stats.discarded,
                        sm::description("Number of hints that were discarded during sending (too old, schema changed, etc.).")),

        sm::make_counter("send_errors", _stats.send_errors,
            sm::description("Number of unexpected errors during sending, sending will be retried later")),

        sm::make_counter("corrupted_files", _stats.corrupted_files,
                        sm::description("Number of hints files that were discarded during sending because the file was corrupted.")),

        sm::make_gauge("pending_drains", 
                        sm::description("Number of tasks waiting in the queue for draining hints"),
                        [this] { return _drain_lock.waiters(); }),

        sm::make_gauge("pending_sends",
                        sm::description("Number of tasks waiting in the queue for sending a hint"),
                        [this] { return _resource_manager.sending_queue_length(); })
    });
}

seastar::future<> manager::start(seastar::shared_ptr<service::storage_proxy> proxy_ptr,
        seastar::shared_ptr<gms::gossiper> gossiper_ptr)
{
    _proxy_anchor = std::move(proxy_ptr);
    _gossiper_anchor = std::move(gossiper_ptr);

    constexpr auto directory_type = seastar::directory_entry_type::directory;
    return lister::scan_dir(_hints_dir, lister::dir_entry_types::of<directory_type>(),
            [this] (fs::path datadir, seastar::directory_entry de) {
        host_id_type ep = host_id_type{de.name};
        if (!check_dc_for(ep)) {
            return seastar::make_ready_future<>();
        }
        return get_host_manager(ep).populate_segments_to_replay();
    }).then([this] {
        return compute_hints_dir_device_id();
    }).then([this] {
        set_started();
    });
}

seastar::future<> manager::stop() {
    manager_logger.info("Asked to stop");

    auto f = seastar::make_ready_future<>();

    return f.finally([this] {
        set_stopping();

        return _draining_hosts_gate.close().finally([this] {
            return seastar::parallel_for_each(_host_managers | boost::adaptors::map_values,
                    [] (host_manager& host_man) {
                return host_man.stop();
            }).finally([this] {
                _host_managers.clear();
                manager_logger.info("Stopped");
            }).discard_result();
        });
    });
}

seastar::future<> manager::compute_hints_dir_device_id() {
    try {
        _hints_dir_device_id = co_await get_device_id(_hints_dir.native());
    } catch (...) {
        manager_logger.warn("Failed to stat directory {} for device id: {}",
                _hints_dir.native(), std::current_exception());
        throw;
    }
}

void manager::allow_hints() {
    // TODO: When std::ranges::values_view is available, replace Boost's adaptor with it.
    std::ranges::for_each(_host_managers | boost::adaptors::map_values, [] (host_manager& hman) {
        hman.allow_hints();
    });
}

void manager::forbid_hints() {
    // TODO: When std::ranges::values_view is available, replace Boost's adaptor with it.
    std::ranges::for_each(_host_managers | boost::adaptors::map_values, [] (host_manager& hman) {
        hman.forbid_hints();
    });
}

void manager::forbid_hints_for_hosts_with_pending_hints() {
    manager_logger.trace("space_watchdog: Going to block hints to: {}", _hosts_with_pending_hints);

    // TODO: When std::ranges::values_view is available, replace Boost's adaptor with it.
    std::ranges::for_each(_host_managers | boost::adaptors::map_values, [this] (host_manager& hman) {
        if (has_host_with_pending_hints(hman.host_id())) {
            hman.forbid_hints();
        } else {
            hman.allow_hints();
        }
    });
}

sync_point::shard_rps manager::calculate_current_sync_point(const std::span<gms::inet_address> target_hosts) const {
    sync_point::shard_rps rps;
    for (auto addr : target_hosts) {
        auto it = _host_managers.find(addr);
        if (it != _host_managers.end()) {
            const host_manager& host_man = it->second;
            rps[host_man.host_id()] = host_man.last_written_replay_position();
        }
    }
    return rps;
}

seastar::future<> manager::wait_for_sync_point(seastar::abort_source& as,
        const sync_point::shard_rps& rps)
{
    abort_source local_as;

    auto sub = as.subscribe([&local_as] () noexcept {
        if (!local_as.abort_requested()) {
            local_as.request_abort();
        }
    });

    if (as.abort_requested()) {
        local_as.request_abort();
    }

    bool was_aborted = false;
    co_await seastar::coroutine::parallel_for_each(_host_managers,
            [&was_aborted, &rps, &local_as] (auto& p) {
        auto& [addr, host_man] = p;

        replay_position rp;
        auto it = rps.find(addr);
        if (it != rps.end()) {
            rp = it->second;
        }

        return host_man.wait_until_hints_are_replayed_up_to(local_as, rp).handle_exception(
                [&local_as, &was_aborted] (auto eptr) {
            if (!local_as.abort_requested()) {
                local_as.request_abort();
            }
            try {
                std::rethrow_exception(std::move(eptr));
            } catch (abort_requested_exception&) {
                was_aborted = true;
            } catch (...) {
                return seastar::make_exception_future<>(std::current_exception());
            }
            return seastar::make_ready_future();
        });
    });

    if (was_aborted) {
        throw seastar::abort_requested_exception{};
    }
}

manager::host_manager& manager::get_host_manager(host_id_type ep) {
    auto it = find_host_manager(ep);
    if (it == host_managers_end()) {
        manager_logger.trace("Creating an ep_manager for {}", ep);
        host_manager& host_man = _host_managers.emplace(ep, host_manager{ep, *this}).first->second;
        host_man.start();
        return host_man;
    }
    return it->second;
}

bool manager::manages_host(host_id_type ep) const noexcept {
    return find_host_manager(ep) != host_managers_end();
}

bool manager::store_hint(host_id_type ep, schema_ptr s, seastar::lw_shared_ptr<const frozen_mutation> fm,
        tracing::trace_state_ptr tr_state) noexcept
{
    if (stopping() || draining_all() || !started() || !can_hint_for(ep)) {
        manager_logger.trace("Can't store a hint to {}", ep);
        ++_stats.dropped;
        return false;
    }

    try {
        manager_logger.trace("Going to store a hint to {}", ep);
        tracing::trace(tr_state, "Going to store a hint to {}", ep);

        return get_host_manager(ep).store_hint(std::move(s), std::move(fm), tr_state);
    } catch (...) {
        manager_logger.trace("Failed to store a hint to {}: {}", ep, std::current_exception());
        tracing::trace(tr_state, "Failed to store a hint to {}: {}", ep, std::current_exception());

        ++_stats.errors;
        return false;
    }
}


bool manager::too_many_in_flight_hints_for(host_id_type ep) const noexcept {
    // There is no need to check the DC here because if there is an in-flight hint
    // for this end point then this means that its DC has already been checked
    // and found to be ok.
    return _stats.size_of_hints_in_progress > max_size_of_hints_in_progress &&
            !utils::fb_utilities::is_me(ep) &&
            hints_in_progress_for(ep) > 0 &&
            local_gossiper().get_endpoint_downtime(ep) <= _max_hint_window_us;
}

bool manager::can_hint_for(host_id_type ep) const noexcept {
    if (utils::fb_utilities::is_me(ep)) {
        return false;
    }

    auto it = find_host_manager(ep);
    if (it != host_managers_end() && (it->second.stopping() || !it->second.can_hint())) {
        return false;
    }

    // Don't allow more than one in-flight (to the store) hint to a specific destination
    // when the total size of in-flight hints is more than the maximum allowed value.
    //
    // In the worst case there's going to be (_max_size_of_hints_in_progress + N - 1) in-flight hints,
    // where N is the total number Nodes in the cluster.
    if (_stats.size_of_hints_in_progress > max_size_of_hints_in_progress && hints_in_progress_for(ep) > 0) {
        manager_logger.trace("size_of_hints_in_progress {} hints_in_progress_for({}) {}",
                _stats.size_of_hints_in_progress, ep, hints_in_progress_for(ep));
        return false;
    }

    // check that the destination DC is "hintable"
    if (!check_dc_for(ep)) {
        manager_logger.trace("{}'s DC is not hintable", ep);
        return false;
    }

    // check if the end point has been down for too long
    if (local_gossiper().get_endpoint_downtime(ep) > _max_hint_window_us) {
        manager_logger.trace("{} is down for {}, not hinting",
                ep, local_gossiper().get_endpoint_downtime(ep));
        return false;
    }

    return true;
}

seastar::future<> manager::change_host_filter(host_filter filter) {
    if (!started()) {
        return seastar::make_exception_future<>(
                std::logic_error{"change_host_filter: called before the hints_manager was started"});
    }

    return seastar::with_gate(_draining_hosts_gate, [this, filter = std::move(filter)] () mutable {
        return seastar::with_semaphore(drain_lock(), 1, [this, filter = std::move(filter)] () mutable {
            if (draining_all()) {
                return seastar::make_exception_future<>(
                        std::logic_error{"change_host_filter: cannot change the configuration because hints all hints were drained"});
            }

            manager_logger.debug("change_host_filter: changing from {} to {}", _host_filter, filter);

            // Change the host_filter now and save the old one so that we can
            // roll back in case of failure
            std::swap(_host_filter, filter);

            constexpr auto directory_type = seastar::directory_entry_type::directory;
            // Iterate over existing hint directories and see if we can enable an endpoint manager
            // for some of them
            return lister::scan_dir(_hints_dir, lister::dir_entry_types::of<directory_type>(),
                    [this] (fs::path datadir, seastar::directory_entry de) {
                const auto ep = host_id_type{de.name};

                if (_host_managers.contains(ep) || !_host_filter.can_hint_for(_proxy_anchor->get_token_metadata_ptr()->get_topology(), ep)) {
                    return seastar::make_ready_future<>();
                }

                return get_host_manager(ep).populate_segments_to_replay();
            }).handle_exception([this, filter = std::move(filter)] (auto ep) mutable {
                // Bring back the old filter. The finally() block will cause us to stop
                // the additional ep_hint_managers that we started
                _host_filter = std::move(filter);
            }).finally([this] {
                // Remove endpoint managers which are rejected by the filter
                return seastar::parallel_for_each(_host_managers, [this] (auto& pair) {
                    auto& [host_id, hman] = pair;

                    if (_host_filter.can_hint_for(_proxy_anchor->get_token_metadata_ptr()->get_topology(), host_id)) {
                        return seastar::make_ready_future<>();
                    }

                    return hman.stop(drain::no).finally([this, host_id = host_id] {
                        _host_managers.erase(host_id);
                    });
                });
            });
        });
    });
}

bool manager::check_dc_for(host_id_type ep) const noexcept {
    try {
        // If target's DC is not a "hintable" DCs - don't hint.
        // If there is an end point manager then DC has already been checked and found to be ok.
        return _host_filter.is_enabled_for_all() || manages_host(ep) ||
               _host_filter.can_hint_for(_proxy_anchor->get_token_metadata_ptr()->get_topology(), ep);
    } catch (...) {
        // if we failed to check the DC - block this hint
        return false;
    }
}

void manager::drain_for(gms::inet_address endpoint) {
    if (!started() || stopping() || draining_all()) {
        return;
    }

    manager_logger.trace("on_leave_cluster: {} is removed/decommissioned", endpoint);

    // Future is waited on indirectly in `stop()` (via `_draining_hosts_gate`).
    (void) seastar::with_gate(_draining_hosts_gate, [this, endpoint] {
        return seastar::with_semaphore(drain_lock(), 1, [this, endpoint] {
            return seastar::futurize_invoke([this, endpoint] {
                if (utils::fb_utilities::is_me(endpoint)) {
                    set_draining_all();

                    // TODO: When std::ranges::values_view is available, replace Boost's adaptor with it.
                    return seastar::parallel_for_each(_host_managers | boost::adaptors::map_values,
                            [] (host_manager& hman) {
                        return hman.stop(drain::yes).finally([&hman] {
                            return hman.with_file_update_mutex([&hman] {
                                return seastar::remove_file(hman.hints_dir().c_str());
                            });
                        });
                    }).finally([this] {
                        _host_managers.clear();
                    });
                } else {
                    auto host_manager_it = find_host_manager(endpoint);
                    if (host_manager_it != host_managers_end()) {
                        auto& hman = host_manager_it->second;

                        return hman.stop(drain::yes).finally([this, endpoint, &hman] {
                            return hman.with_file_update_mutex([&hman] {
                                return seastar::remove_file(hman.hints_dir().c_str());
                            }).finally([this, endpoint] {
                                _host_managers.erase(endpoint);
                            });
                        });
                    }

                    return seastar::make_ready_future<>();
                }
            }).handle_exception([endpoint] (auto eptr) {
                manager_logger.error("Exception when draining {}: {}", endpoint, eptr);
            });
        });
    }).finally([endpoint] {
        manager_logger.trace("drain_for: finished draining {}", endpoint);
    });
}

static seastar::future<> scan_for_hints_dirs(const seastar::sstring& hints_directory,
        std::function<seastar::future<> (fs::path dir, seastar::directory_entry de, seastar::shard_id shard_id)> f)
{
    constexpr auto directory_type = seastar::directory_entry_type::directory;
    return lister::scan_dir(hints_directory, lister::dir_entry_types::of<directory_type>(),
            [f = std::move(f)] (fs::path dir, seastar::directory_entry de) mutable {
        seastar::shard_id shard_id;
        try {
            shard_id = std::stoi(de.name.c_str());
        } catch (std::invalid_argument& ex) {
            manager_logger.debug("Ignore invalid directory {}", de.name);
            return seastar::make_ready_future<>();
        }
        return f(std::move(dir), std::move(de), shard_id);
    });
}

// runs in seastar::async context
manager::hints_segments_map manager::get_current_hints_segments(const seastar::sstring& hints_directory) {
    hints_segments_map current_hints_segments;
    constexpr auto directory_type = seastar::directory_entry_type::directory;

    // shards level
    scan_for_hints_dirs(hints_directory,
            [&current_hints_segments] (fs::path dir, seastar::directory_entry de, seastar::shard_id shard_id) {
        manager_logger.trace("shard_id = {}", shard_id);
        // IPs level
        return lister::scan_dir(dir / de.name.c_str(), lister::dir_entry_types::of<directory_type>(),
                [&current_hints_segments, shard_id] (fs::path dir, seastar::directory_entry de) {
            manager_logger.trace("\tIP: {}", de.name);
            // hints files
            return lister::scan_dir(dir / de.name.c_str(), lister::dir_entry_types::of<directory_type>(),
                    [&current_hints_segments, shard_id, ep_addr = de.name] (fs::path dir, seastar::directory_entry de) {
                manager_logger.trace("\t\tfile: {}", de.name);
                current_hints_segments[ep_addr][shard_id].emplace_back(dir / de.name.c_str());
                return seastar::make_ready_future<>();
            });
        });
    }).get();

    return current_hints_segments;
}

// runs in seastar::async context
void manager::rebalance_segments(const seastar::sstring& hints_directory, hints_segments_map& segments_map) {
    // Count how many hints segments to each destination we have.
    std::unordered_map<seastar::sstring, size_t> per_ep_hints;
    for (auto& ep_info : segments_map) {
        per_ep_hints[ep_info.first] = boost::accumulate(
                ep_info.second |
                boost::adaptors::map_values |
                boost::adaptors::transformed(std::mem_fn(&std::list<fs::path>::size)),
                0);
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
            rebalance_segments_for(ep, q, hints_directory,
                    current_segments_map, current_segments_to_move);
        }

        if (r) {
            rebalance_segments_for(ep, q + 1, hints_directory,
                    current_segments_map, current_segments_to_move);
        }
    }
}

// runs in seastar::async context
void manager::rebalance_segments_for(const seastar::sstring& ep, size_t segments_per_shard,
        const seastar::sstring& hints_directory, hints_host_segments_map& ep_segments,
        std::list<fs::path>& segments_to_move)
{
    manager_logger.trace("{}: segments_per_shard: {}, total number of segments to move: {}",
            ep, segments_per_shard, segments_to_move.size());

    // sanity check
    if (segments_to_move.empty() || !segments_per_shard) {
        return;
    }

    for (seastar::shard_id i = 0; i < smp::count && !segments_to_move.empty(); ++i) {
        fs::path shard_path_dir{fs::path{hints_directory.c_str()} / seastar::format("{:d}", i).c_str() / ep.c_str()};
        std::list<fs::path>& current_shard_segments = ep_segments[i];

        // Make sure that the shard_path_dir exists and if not - create it
        io_check([name = shard_path_dir.c_str()] {
            return seastar::recursive_touch_directory(name);
        }).get();

        while (current_shard_segments.size() < segments_per_shard && !segments_to_move.empty()) {
            auto seg_path_it = segments_to_move.begin();
            fs::path new_path{shard_path_dir / seg_path_it->filename()};

            // Don't move the file to the same location - it's pointless.
            if (*seg_path_it != new_path) {
                manager_logger.trace("going to move: {} -> {}", *seg_path_it, new_path);
                io_check(seastar::rename_file, seg_path_it->native(), new_path.native()).get();
            } else {
                manager_logger.trace("skipping: {}", *seg_path_it);
            }
            current_shard_segments.splice(current_shard_segments.end(), segments_to_move,
                    seg_path_it, std::next(seg_path_it));
        }
    }
}

// runs in seastar::async context
void manager::remove_irrelevant_shards_directories(const seastar::sstring& hints_directory) {
    // Shards level
    scan_for_hints_dirs(hints_directory, [] (fs::path dir, seastar::directory_entry de, seastar::shard_id shard_id)
            -> seastar::future<>
    {
        if (shard_id >= smp::count) {
            // IPs level
            co_await lister::scan_dir(
                    dir / de.name.c_str(),
                    lister::dir_entry_types::full(),
                    lister::show_hidden::yes,
                    [] (fs::path dir, seastar::directory_entry de) {
                        return io_check(seastar::remove_file, (dir / de.name.c_str()).native());
                    });
            
            // Specific shard level
            co_await io_check(seastar::remove_file, (dir / de.name.c_str()).native());
        }
    }).get();
}

seastar::future<> manager::rebalance(seastar::sstring hints_directory) {
    return seastar::async([hints_directory = std::move(hints_directory)] {
        // Scan currently present hints segments.
        hints_segments_map current_hints_segments = get_current_hints_segments(hints_directory);

        // Move segments to achieve an even distribution of files among all present shards.
        rebalance_segments(hints_directory, current_hints_segments);

        // Remove the directories of shards that are not present anymore
        // -- they should not have any segments by now.
        remove_irrelevant_shards_directories(hints_directory);
    });
}

void manager::update_backlog(size_t backlog, size_t max_backlog) {
    if (backlog < max_backlog) {
        allow_hints();
    } else {
        forbid_hints_for_hosts_with_pending_hints();
    }
}

class directory_initializer::impl {
private:
    enum class state {
        uninitialized = 0,
        created_and_validated = 1,
        rebalanced = 2,
    };

private:
    utils::directories& _dirs;
    seastar::sstring _hints_directory;
    state _state = state::uninitialized;
    seastar::named_semaphore _lock = {1, named_semaphore_exception_factory{"hints directory initialization lock"}};

public:
    impl(utils::directories& dirs, seastar::sstring hints_directory)
        : _dirs(dirs)
        , _hints_directory(std::move(hints_directory))
    {}

public:
    seastar::future<> ensure_created_and_verified() {
        if (_state > state::uninitialized) {
            co_return;
        }

        const auto sem_units = co_await seastar::get_units(_lock, 1);
        
        utils::directories::set dir_set;
        dir_set.add_sharded(_hints_directory);
        
        co_await _dirs.create_and_verify(std::move(dir_set));
        manager_logger.debug("Creating and validating hint directories: {}", _hints_directory);
        
        _state = state::created_and_validated;
    }

    seastar::future<> ensure_rebalanced() {
        if (_state < state::created_and_validated) {
            throw std::logic_error{"hints directory needs to be created and validated before rebalancing"};
        }

        if (_state > state::created_and_validated) {
            co_return;
        }

        const auto sem_units = co_await seastar::get_units(_lock, 1);
        
        manager_logger.debug("Rebalancing hints in {}", _hints_directory);
        co_await manager::rebalance(_hints_directory);
        
        _state = state::rebalanced;
    }
};

seastar::future<directory_initializer> directory_initializer::make(utils::directories& dirs,
        seastar::sstring hints_directory)
{
    return smp::submit_to(0, [&dirs, hints_directory = std::move(hints_directory)] () mutable {
        auto impl = std::make_shared<directory_initializer::impl>(dirs, std::move(hints_directory));
        return seastar::make_ready_future<directory_initializer>(directory_initializer{std::move(impl)});
    });
}

seastar::future<> directory_initializer::ensure_created_and_verified() {
    if (!_impl) {
        return seastar::make_ready_future<>();
    }
    return smp::submit_to(0, [impl = _impl] () mutable {
        return impl->ensure_created_and_verified().then([impl] {});
    });
}

seastar::future<> directory_initializer::ensure_rebalanced() {
    if (!_impl) {
        return seastar::make_ready_future<>();
    }
    return smp::submit_to(0, [impl = _impl] () mutable {
        return impl->ensure_rebalanced().then([impl] {});
    });
}

} // namespace db::hints
