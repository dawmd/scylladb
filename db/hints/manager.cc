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
#include "utils/div_ceil.hh"
#include "utils/error_injection.hh"
#include "utils/exceptions.hh"
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


///////////////////////////
///////////////////////////


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

bool manager::can_hint_for(host_id_type ep) const noexcept {
    if (utils::fb_utilities::is_me(ep)) {
        return false;
    }

    auto it = _host_managers.find(ep);
    if (it != _host_managers.end() && (it->second.stopping() || !it->second.can_hint())) {
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

bool manager::check_dc_for(host_id_type ep) const noexcept {
    try {
        const auto& topology = _proxy_anchor->get_token_metadata_ptr()->get_topology();
        // If target's DC is not a "hintable" DCs - don't hint.
        // If there is an end point manager then DC has already been checked and found to be ok.
        return _host_filter.is_enabled_for_all() ||
                manages_host(ep) ||
               _host_filter.can_hint_for(topology, ep);
    } catch (...) {
        // if we failed to check the DC - block this hint
        return false;
    }
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
                    auto host_manager_it = _host_managers.find(endpoint);
                    if (host_manager_it != _host_managers.end()) {
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

sync_point::shard_rps manager::calculate_current_sync_point(std::span<const gms::inet_address> target_hosts) const {
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

bool manager::too_many_in_flight_hints_for(host_id_type ep) const noexcept {
    // There is no need to check the DC here because if there is an in-flight hint
    // for this end point then this means that its DC has already been checked
    // and found to be ok.
    return _stats.size_of_hints_in_progress > max_size_of_hints_in_progress &&
            !utils::fb_utilities::is_me(ep) &&
            hints_in_progress_for(ep) > 0 &&
            local_gossiper().get_endpoint_downtime(ep) <= _max_hint_window_us;
}

seastar::future<> manager::change_host_filter(host_filter filter) {
    if (!started()) {
        throw std::logic_error{"change_host_filter: called before the hints_manager was started"};
    }

    const auto gate_holder = seastar::gate::holder{_draining_hosts_gate};
    const auto units = co_await seastar::get_units(drain_lock(), 1);
    
    if (draining_all()) {
        throw std::logic_error{"change_host_filter: cannot change the configuration because hints all hints were drained"};
    }

    manager_logger.debug("change_host_filter: changing from {} to {}", _host_filter, filter);

    // Change the host_filter now and save the old one so that we can
    // roll back in case of failure
    std::swap(_host_filter, filter);

    std::exception_ptr eptr = nullptr;

    try {
        constexpr auto directory_type = seastar::directory_entry_type::directory;
        // Iterate over existing hint directories and see if we can enable an endpoint manager
        // for some of them
        co_await lister::scan_dir(_hints_dir, lister::dir_entry_types::of<directory_type>(),
                [this] (fs::path datadir, seastar::directory_entry de) {
            const auto ep = host_id_type{de.name};
            const auto& topology = _proxy_anchor->get_token_metadata_ptr()->get_topology();

            if (_host_managers.contains(ep) || !_host_filter.can_hint_for(topology, ep)) {
                return seastar::make_ready_future<>();
            }

            return get_host_manager(ep).populate_segments_to_replay();
        });
    } catch (...) {
        // Bring back the old filter. The finally() block will cause us to stop
        // the additional ep_hint_managers that we started
        _host_filter = std::move(filter);
        eptr = std::current_exception();
    }
    
    try {
        // Remove endpoint managers which are rejected by the filter
        co_await seastar::coroutine::parallel_for_each(_host_managers, [this] (auto& pair) {
            auto& [host_id, hman] = pair;
            const auto& topology = _proxy_anchor->get_token_metadata_ptr()->get_topology();

            if (_host_filter.can_hint_for(topology, host_id)) {
                return seastar::make_ready_future<>();
            }

            return hman.stop(drain::no).finally([this, host_id = host_id] {
                _host_managers.erase(host_id);
            });
        });
    } catch (...) {
        eptr = make_nested_exception_ptr(std::current_exception(), eptr);
    }

    if (eptr) {
        std::rethrow_exception(eptr);
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

seastar::future<> manager::rebalance(seastar::sstring hints_directory) {
    co_await rebalance_hints(std::move(hints_directory));
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

manager::host_manager& manager::get_host_manager(host_id_type ep) {
    auto it = _host_managers.find(ep);
    if (it == _host_managers.end()) {
        manager_logger.trace("Creating an ep_manager for {}", ep);
        host_manager& host_man = _host_managers.emplace(ep, host_manager{ep, *this}).first->second;
        host_man.start();
        return host_man;
    }
    return it->second;
}

bool manager::manages_host(host_id_type ep) const noexcept {
    return _host_managers.contains(ep);
}

void manager::update_backlog(size_t backlog, size_t max_backlog) {
    if (backlog < max_backlog) {
        allow_hints();
    } else {
        forbid_hints_for_hosts_with_pending_hints();
    }
}

} // namespace db::hints
