/*
 * Modified by ScyllaDB
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

// Seastar features.
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/shared_mutex.hh>
#include <seastar/core/abort_source.hh>

// Scylla includes.
#include "db/commitlog/commitlog.hh"
#include "db/hints/internal/common.hh"
#include "db/hints/internal/host_manager.hh"
#include "db/hints/host_filter.hh"
#include "db/hints/resource_manager.hh"
#include "db/hints/sync_point.hh"
#include "utils/loading_shared_values.hh"
#include "inet_address_vectors.hh"

// STD.
#include <chrono>
#include <filesystem>
#include <list>
#include <map>
#include <optional>
#include <unordered_map>
#include <vector>

class fragmented_temporary_buffer;

namespace utils {
class directories;
}

namespace gms {
class gossiper;
}

namespace db::hints {

/// A helper class which tracks hints directory creation
/// and allows to perform hints directory initialization lazily.
class directory_initializer {
private:
    class impl;
    std::shared_ptr<impl> _impl;

    directory_initializer(std::shared_ptr<impl> impl);

public:
    /// Creates an initializer that does nothing. Useful in tests.
    static directory_initializer make_dummy();
    static seastar::future<directory_initializer> make(utils::directories& dirs, seastar::sstring hints_directory);

    ~directory_initializer();
    seastar::future<> ensure_created_and_verified();
    seastar::future<> ensure_rebalanced();
};

class manager {
private:
    friend class space_watchdog;
    // TODO: Think if giving `internal::host_manager` full access to this class
    //       is okay.
    friend class internal::host_manager;

    enum class state {
        started,                // hinting is currently allowed (start() call is complete)
        replay_allowed,         // replaying (hints sending) is allowed
        draining_all,           // hinting is not allowed - all ep managers are being stopped because this node is leaving the cluster
        stopping                // hinting is not allowed - stopping is in progress (stop() method has been called)
    };

    using state_set = enum_set<super_enum<state,
        state::started,
        state::replay_allowed,
        state::draining_all,
        state::stopping>>;

    using host_manager_map = std::unordered_map<internal::host_id_type, internal::host_manager>;

private:
    static constexpr uint64_t MAX_SIZE_OF_HINTS_IN_PROGRESS = 10 * 1024 * 1024; // 10 MB

// Fields.
private:
    state_set _state;
    std::filesystem::path _hints_dir;
    dev_t _hints_dir_device_id = 0;

    internal::node_to_hint_store_factory_type _store_factory;
    host_filter _host_filter;
    seastar::shared_ptr<service::storage_proxy> _proxy_anchor;
    seastar::shared_ptr<gms::gossiper> _gossiper_anchor;
    int64_t _max_hint_window_us = 0;
    replica::database& _local_db;

    seastar::gate _draining_eps_gate; // gate used to control the progress of ep_managers stopping not in the context of manager::stop() call

    resource_manager& _resource_manager;

    host_manager_map _host_managers;
    internal::hint_stats _stats;
    seastar::metrics::metric_groups _metrics;
    std::unordered_set<internal::host_id_type> _hosts_with_pending_hints;
    seastar::named_semaphore _drain_lock = {1, named_semaphore_exception_factory{"drain lock"}};

public:
    manager(seastar::sstring hints_directory, host_filter filter, int64_t max_hint_window_ms,
            resource_manager& res_manager, seastar::sharded<replica::database>& db);
    virtual ~manager();
    manager(manager&&) = delete;
    manager& operator=(manager&&) = delete;
    void register_metrics(const sstring& group_name);
    future<> start(shared_ptr<service::storage_proxy> proxy_ptr, shared_ptr<gms::gossiper> gossiper_ptr);
    future<> stop();
    bool store_hint(gms::inet_address ep, schema_ptr s, lw_shared_ptr<const frozen_mutation> fm, tracing::trace_state_ptr tr_state) noexcept;

    /// \brief Changes the host_filter currently used, stopping and starting ep_managers relevant to the new host_filter.
    /// \param filter the new host_filter
    /// \return A future that resolves when the operation is complete.
    future<> change_host_filter(host_filter filter);

    const host_filter& get_host_filter() const noexcept {
        return _host_filter;
    }

    /// \brief Check if a hint may be generated to the give end point
    /// \param ep end point to check
    /// \return true if we should generate the hint to the given end point if it becomes unavailable
    bool can_hint_for(internal::host_id_type ep) const noexcept;

    /// \brief Check if there aren't too many in-flight hints
    ///
    /// This function checks if there are too many "in-flight" hints on the current shard - hints that are being stored
    /// and which storing is not complete yet. This is meant to stabilize the memory consumption of the hints storing path
    /// which is initialed from the storage_proxy WRITE flow. storage_proxy is going to check this condition and if it
    /// returns TRUE it won't attempt any new WRITEs thus eliminating the possibility of new hints generation. If new hints
    /// are not generated the amount of in-flight hints amount and thus the memory they are consuming is going to drop eventualy
    /// because the hints are going to be either stored or dropped. After that the things are going to get back to normal again.
    ///
    /// Note that we can't consider the disk usage consumption here because the disk usage is not promissed to drop down shortly
    /// because it requires the remote node to be UP.
    ///
    /// \param ep end point to check
    /// \return TRUE if we are allowed to generate hint to the given end point but there are too many in-flight hints
    bool too_many_in_flight_hints_for(internal::host_id_type ep) const noexcept;

    /// \brief Check if DC \param ep belongs to is "hintable"
    /// \param ep End point identificator
    /// \return TRUE if hints are allowed to be generated to \param ep.
    bool check_dc_for(internal::host_id_type ep) const noexcept;

    /// \brief Checks if hints are disabled for all endpoints
    /// \return TRUE if hints are disabled.
    bool is_disabled_for_all() const noexcept {
        return _host_filter.is_disabled_for_all();
    }

    /// \return Size of mutations of hints in-flight (to the disk) at the moment.
    uint64_t size_of_hints_in_progress() const noexcept {
        return _stats.size_of_hints_in_progress;
    }

    /// \brief Get the number of in-flight (to the disk) hints to a given end point.
    /// \param ep End point identificator
    /// \return Number of hints in-flight to \param ep.
    uint64_t hints_in_progress_for(internal::host_id_type ep) const noexcept {
        auto it = _host_managers.find(ep);
        if (it == _host_managers.end()) {
            return 0;
        }
        return it->second.hints_in_progress();
    }

    void add_ep_with_pending_hints(internal::host_id_type key) {
        _hosts_with_pending_hints.insert(key);
    }

    void clear_hosts_with_pending_hints() {
        _hosts_with_pending_hints.clear();
        _hosts_with_pending_hints.reserve(_host_managers.size());
    }

    bool has_ep_with_pending_hints(internal::host_id_type key) const {
        return _hosts_with_pending_hints.contains(key);
    }

    size_t ep_managers_size() const {
        return _host_managers.size();
    }

    const fs::path& hints_dir() const {
        return _hints_dir;
    }

    dev_t hints_dir_device_id() const {
        return _hints_dir_device_id;
    }

    seastar::named_semaphore& drain_lock() noexcept {
        return _drain_lock;
    }

    void allow_hints();
    void forbid_hints();
    void forbid_hints_for_hosts_with_pending_hints();

    void allow_replaying() noexcept {
        _state.set(state::replay_allowed);
    }

    /// \brief Returns a set of replay positions for hint queues towards endpoints from the `target_hosts`.
    sync_point::shard_rps calculate_current_sync_point(const std::vector<internal::host_id_type>& target_hosts) const;

    /// \brief Waits until hint replay reach replay positions described in `rps`.
    future<> wait_for_sync_point(abort_source& as, const sync_point::shard_rps& rps);

    /// \brief Rebalance hints segments among all present shards.
    ///
    /// The difference between the number of segments on every two shard will be not greater than 1 after the
    /// rebalancing.
    ///
    /// Removes the sub-directories of \ref hints_directory that correspond to shards that are not relevant any more
    /// (re-sharding to a lower shards number case).
    ///
    /// Complexity: O(N+K), where N is a total number of present hints' segments and
    ///                           K = <number of shards during the previous boot> * <number of end points for which hints where ever created>
    ///
    /// \param hints_directory A hints directory to rebalance
    /// \return A future that resolves when the operation is complete.
    static future<> rebalance(sstring hints_directory);

    // TODO: Change this comment to match the current behavior.
    //
    /// \brief Safely runs a given functor under the file_update_mutex of \ref ep_man
    ///
    /// Runs a given functor under the file_update_mutex of the given end_point_hints_manager instance.
    /// This function is safe even if \ref ep_man gets destroyed before the future this function returns resolves
    /// (as long as the \ref func call itself is safe).
    ///
    /// \tparam Func Functor type.
    /// \param ep_man end_point_hints_manager instance which file_update_mutex we want to lock.
    /// \param func Functor to run under the lock.
    /// \return Whatever \ref func returns.
    template<typename Func>
    decltype(auto) with_file_update_mutex(internal::host_id_type ep, Func&& func) {
        auto mutex_ptr = get_file_update_mutex_ptr(ep);
        return seastar::with_lock(*mutex_ptr, std::forward<Func>(func)).finally([mutex_ptr] {
            // Extend the lifetime.
        });
    }

private:
    seastar::lw_shared_ptr<seastar::shared_mutex> get_file_update_mutex_ptr(internal::host_id_type ep);

    future<> compute_hints_dir_device_id();

    internal::node_to_hint_store_factory_type& store_factory() noexcept {
        return _store_factory;
    }

    service::storage_proxy& local_storage_proxy() const noexcept {
        return *_proxy_anchor;
    }

    gms::gossiper& local_gossiper() const noexcept {
        return *_gossiper_anchor;
    }

    replica::database& local_db() noexcept {
        return _local_db;
    }

    bool have_ep_manager(internal::host_id_type ep) const noexcept;
    internal::host_manager& get_host_manager(internal::host_id_type ep);

public:
    /// \brief Initiate the draining when we detect that the node has left the cluster.
    ///
    /// If the node that has left is the current node - drains all pending hints to all nodes.
    /// Otherwise drains hints to the node that has left.
    ///
    /// In both cases - removes the corresponding hints' directories after all hints have been drained and erases the
    /// corresponding end_point_hints_manager objects.
    ///
    /// \param endpoint node that left the cluster
    void drain_for(gms::inet_address endpoint);

private:
    void update_backlog(size_t backlog, size_t max_backlog);

    bool stopping() const noexcept {
        return _state.contains(state::stopping);
    }

    void set_stopping() noexcept {
        _state.set(state::stopping);
    }

    bool started() const noexcept {
        return _state.contains(state::started);
    }

    void set_started() noexcept {
        _state.set(state::started);
    }

    bool replay_allowed() const noexcept {
        return _state.contains(state::replay_allowed);
    }

    void set_draining_all() noexcept {
        _state.set(state::draining_all);
    }

    bool draining_all() noexcept {
        return _state.contains(state::draining_all);
    }
};

} // namespace db::hints
