/*
 * Copyright (C) 2018-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "resource_manager.hh"

// Seastar features.
#include <seastar/core/sleep.hh>
#include <seastar/core/seastar.hh>

// Boost features.
#include <boost/range/algorithm/for_each.hpp>
#include <boost/range/adaptor/map.hpp>

// Scylla includes.
#include "seastar/core/loop.hh"
#include "utils/UUID.hh"
#include "utils/disk-error-handler.hh"
#include "utils/div_ceil.hh"
#include "utils/lister.hh"
#include "log.hh"
#include "manager.hh"
#include "seastarx.hh"

namespace db::hints {

static logging::logger resource_manager_logger{"hints_resource_manager"};

seastar::future<dev_t> get_device_id(const fs::path& path) {
    const auto sd = co_await seastar::file_stat(path.native());
    co_return sd.device_id;
}

seastar::future<bool> is_mountpoint(const fs::path& path) {
    // Special case for '/', which is always a mount point
    if (path == path.parent_path()) {
        co_return true;
    }

    const auto id1 = co_await get_device_id(path);
    const auto id2 = co_await get_device_id(path.parent_path());

    co_return id1 != id2;
}

seastar::future<seastar::semaphore_units<seastar::named_semaphore::exception_factory>> resource_manager::get_send_units_for(size_t buf_size) {
    // In order to impose a limit on the number of hints being sent concurrently,
    // require each hint to reserve at least 1/(max concurrency) of the shard budget
    const size_t per_node_concurrency_limit = _max_hints_send_queue_length();
    const size_t per_shard_concurrency_limit = (per_node_concurrency_limit > 0)
            ? div_ceil(per_node_concurrency_limit, smp::count)
            : default_per_shard_concurrency_limit;
    const size_t min_send_hint_budget = _max_send_in_flight_memory / per_shard_concurrency_limit;
    // Let's approximate the memory size the mutation is going to consume by the size of its serialized form
    size_t hint_memory_budget = std::max(min_send_hint_budget, buf_size);
    // Allow a very big mutation to be sent out by consuming the whole shard budget
    hint_memory_budget = std::min(hint_memory_budget, _max_send_in_flight_memory);
    resource_manager_logger.trace("memory budget: need {} have {}", hint_memory_budget, _send_limiter.available_units());
    return get_units(_send_limiter, hint_memory_budget);
}

size_t resource_manager::sending_queue_length() const {
    return _send_limiter.waiters();
}

const std::chrono::seconds space_watchdog::_watchdog_period = std::chrono::seconds(1);

space_watchdog::space_watchdog(shard_managers_set& managers, per_device_limits_map& per_device_limits_map)
    : _shard_managers{managers}
    , _per_device_limits_map{per_device_limits_map}
    , _update_lock{1, seastar::named_semaphore_exception_factory{"update lock"}}
{}

void space_watchdog::start() {
    _started = seastar::async([this] {
        while (!_as.abort_requested()) {
            try {
                const auto units = get_units(_update_lock, 1).get();
                on_timer();
            } catch (...) {
                resource_manager_logger.trace("space_watchdog: unexpected exception - stop all hints generators");
                // Stop all hint generators if space_watchdog callback failed
                for (manager& shard_manager : _shard_managers) {
                    shard_manager.forbid_hints();
                }
            }
            seastar::sleep_abortable(_watchdog_period, _as).get();
        }
    }).handle_exception_type([] (const seastar::sleep_aborted& ignored) { });
}

seastar::future<> space_watchdog::stop() noexcept {
    _as.request_abort();
    return std::move(_started);
}

// Called under the end_point_hints_manager::file_update_mutex() of the corresponding end_point_hints_manager instance.
seastar::future<> space_watchdog::scan_one_host_dir(fs::path path, manager& shard_manager, host_id_type host_id) {
    // It may happen that we get here and the directory has already been deleted in the context of manager::drain_for().
    // In this case simply bail out.
    if (!co_await seastar::file_exists(path.native())) {
        co_return;
    }

    namespace sc = seastar::coroutine;
    co_await lister::scan_dir(path, lister::dir_entry_types::of<directory_entry_type::regular>(),
            sc::lambda([this, host_id, &shard_manager] (fs::path dir, seastar::directory_entry de) -> seastar::future<> {
        // Put the current end point ID to state.eps_with_pending_hints when we see the second hints file in its directory
        if (_files_count == 1) {
            shard_manager.add_host_with_pending_hints(host_id);
        }
        ++_files_count;

        _total_size += co_await io_check(file_size, (dir / de.name.c_str()).c_str());
    }));
}

// Called from the context of a seastar::thread.
void space_watchdog::on_timer() {
    // The hints directories are organized as follows:
    // <hints root>
    //    |- <shard1 ID>
    //    |  |- <EP1 address>
    //    |     |- <hints file1>
    //    |     |- <hints file2>
    //    |     |- ...
    //    |  |- <EP2 address>
    //    |     |- ...
    //    |  |-...
    //    |- <shard2 ID>
    //    |  |- ...
    //    ...
    //    |- <shardN ID>
    //    |  |- ...
    //

    for (auto& per_device_limits : _per_device_limits_map | boost::adaptors::map_values) {
        _total_size = 0;
        for (manager& shard_manager : per_device_limits.managers) {
            shard_manager.clear_hosts_with_pending_hints();
            lister::scan_dir(shard_manager.hints_dir(), lister::dir_entry_types::of<directory_entry_type::directory>(),
                    [this, &shard_manager] (fs::path dir, directory_entry de) {
                _files_count = 0;
                // Let's scan per-end-point directories and enumerate hints files...
                //
                // Let's check if there is a corresponding end point manager (may not exist if the corresponding DC is
                // not hintable).
                // If exists - let's take a file update lock so that files are not changed under our feet. Otherwise, simply
                // continue to enumeration - there is no one to change them.
                const auto host_id = locator::host_id{utils::UUID{de.name}};

                auto it = shard_manager.find_host_manager(host_id);
                if (it != shard_manager.host_managers_end()) {
                    return with_file_update_mutex(it->second, [this, &shard_manager, dir = std::move(dir), ep_name = std::move(de.name), host_id] () mutable {
                        return scan_one_host_dir(dir / ep_name, shard_manager, host_id);
                    });
                } else {
                    return scan_one_host_dir(dir / de.name, shard_manager, host_id);
                }
            }).get();
        }

        // Adjust the quota to take into account the space we guarantee to every end point manager
        size_t adjusted_quota = 0;
        size_t delta = boost::accumulate(per_device_limits.managers, 0, [] (size_t sum, manager& shard_manager) {
            return sum + shard_manager.host_managers_size() * resource_manager::hint_segment_size_in_mb * 1024 * 1024;
        });
        if (per_device_limits.max_shard_disk_space_size > delta) {
            adjusted_quota = per_device_limits.max_shard_disk_space_size - delta;
        }

        resource_manager_logger.trace("space_watchdog: consuming {}/{} bytes", _total_size, adjusted_quota);
        for (manager& shard_manager : per_device_limits.managers) {
            shard_manager.update_backlog(_total_size, adjusted_quota);
        }
    }
}

seastar::future<> resource_manager::start(seastar::shared_ptr<service::storage_proxy> proxy_ptr,
        seastar::shared_ptr<gms::gossiper> gossiper_ptr)
{
    _proxy_ptr = std::move(proxy_ptr);
    _gossiper_ptr = std::move(gossiper_ptr);

    namespace sc = seastar::coroutine;
    co_await seastar::with_semaphore(_operation_lock, 1, sc::lambda([this] () -> seastar::future<> {
        co_await seastar::parallel_for_each(_shard_managers, sc::lambda([this] (manager& m) {
            return m.start(_proxy_ptr, _gossiper_ptr);
        }));
        
        co_await seastar::do_for_each(_shard_managers, sc::lambda([this] (manager& m) {
            return prepare_per_device_limits(m);
        }));
        
        _space_watchdog.start();
        set_running();
    }));
}

void resource_manager::allow_replaying() noexcept {
    set_replay_allowed();
    boost::for_each(_shard_managers, [] (manager& m) { m.allow_replaying(); });
}

seastar::future<> resource_manager::stop() noexcept {
    return seastar::with_semaphore(_operation_lock, 1, [this] {
        return seastar::parallel_for_each(_shard_managers, [] (manager& m) {
            return m.stop();
        }).finally([this] {
            return _space_watchdog.stop();
        }).then([this] {
            unset_running();
        });
    });
}

seastar::future<> resource_manager::register_manager(manager& m) {
    namespace sc = seastar::coroutine;
    co_await seastar::with_semaphore(_operation_lock, 1,
            sc::lambda([this, &m] () -> seastar::future<> {
        co_await seastar::with_semaphore(_space_watchdog.update_lock(), 1,
                sc::lambda([this, &m] () -> seastar::future<> {
            const auto [it, inserted] = _shard_managers.insert(m);
            if (!inserted) {
                // Already registered
                co_return;
            }
            if (!running()) {
                // The hints manager will be started later by resource_manager::start()
                co_return;
            }

            // If the resource_manager was started, start the hints manager, too.
            try {
                co_await m.start(_proxy_ptr, _gossiper_ptr);
                // Calculate device limits for this manager so that it is accounted for
                // by the space_watchdog
                co_await prepare_per_device_limits(m);
                if (this->replay_allowed()) {
                    m.allow_replaying();
                }
            } catch (...) {
                _shard_managers.erase(m);
                throw;
            }
        }));
    }));
}

seastar::future<> resource_manager::prepare_per_device_limits(manager& shard_manager) {
    const dev_t device_id = shard_manager.hints_dir_device_id();
    auto it = _per_device_limits_map.find(device_id);
    if (it == _per_device_limits_map.end()) {
        const bool is_mp = co_await is_mountpoint(shard_manager.hints_dir().parent_path());
        auto [it, inserted] = _per_device_limits_map.emplace(device_id, space_watchdog::per_device_limits{});
        // Since we possibly deferred, we need to recheck the _per_device_limits_map.
        if (inserted) {
            // By default, give each group of managers 10% of the available disk space. Give each shard an equal share of the available space.
            it->second.max_shard_disk_space_size = std::filesystem::space(shard_manager.hints_dir().c_str()).capacity / (10 * smp::count);
            // If hints directory is a mountpoint, we assume it's on dedicated (i.e. not shared with data/commitlog/etc) storage.
            // Then, reserve 90% of all space instead of 10% above.
            if (is_mp) {
                it->second.max_shard_disk_space_size *= 9;
            }
        }
        it->second.managers.emplace_back(std::ref(shard_manager));
    } else {
        it->second.managers.emplace_back(std::ref(shard_manager));
    }
}

} // namespace db::hints
