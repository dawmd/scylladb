/*
 * Modified by ScyllaDB
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

// Seastar features.
#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_mutex.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/bool_class.hh>

// Scylla includes.
#include "db/commitlog/commitlog.hh"
#include "db/commitlog/commitlog_entry.hh"
#include "db/commitlog/replay_position.hh"
#include "db/hints/host_filter.hh"
#include "db/hints/resource_manager.hh"
#include "db/hints/sync_point.hh"
#include "enum_set.hh"
#include "gc_clock.hh"
#include "gms/gossiper.hh"
#include "inet_address_vectors.hh"
#include "locator/host_id.hh"
#include "mutation/frozen_mutation.hh"
#include "replica/database.hh"
#include "schema/schema.hh"
#include "schema/schema_fwd.hh"
#include "tracing/trace_state.hh"
#include "utils/directories.hh"
#include "utils/fragmented_temporary_buffer.hh"
#include "utils/loading_shared_values.hh"

// STD.
#include <chrono>
#include <list>
#include <memory>
#include <optional>
#include <set>
#include <span>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace db::hints {

class directory_initializer {
private:
    class impl;
private:
    std::shared_ptr<impl> _impl;

private:
    directory_initializer(std::shared_ptr<impl> impl);
public:
    ~directory_initializer();

public:
    static directory_initializer make_dummy() noexcept {
        return {nullptr};
    }

    static seastar::future<directory_initializer> make(utils::directories& dirs,
            seastar::sstring hints_directory);

    seastar::future<> ensure_created_and_verified();
    seastar::future<> ensure_rebalanced();
};

class no_host_found : public std::exception {
public:
    const char* what() const noexcept override {
        return "No host known of the specified ID";
    }
};

namespace internal {
class manager_impl;
} // namesapce internal

class manager {
public:
    static constexpr uint64_t max_size_of_hints_in_progress = 10 * 1024 * 1024; // 10 MB

private:
    std::unique_ptr<internal::manager_impl> _impl;

public:
    manager(seastar::sstring hints_directory, host_filter filter, int64_t max_hint_window_ms,
            resource_manager& res_manager, seastar::sharded<replica::database>& db);
    manager(manager&&) = delete;
    ~manager() noexcept;

public:
    seastar::future<> start(seastar::shared_ptr<service::storage_proxy> proxy_ptr,
            seastar::shared_ptr<gms::gossiper> gossiper_ptr);
    seastar::future<> stop();

    void register_metrics(const seastar::sstring& group_name);

    bool store_hint(const locator::host_id& host_id, schema_ptr s,
            seastar::lw_shared_ptr<const frozen_mutation> fm,
            tracing::trace_state_ptr tr_state) noexcept;
    
    /// \brief Changes the host_filter currently used, stopping and starting host_hint_managers relevant to the new host_filter.
    /// \param filter the new host_filter
    /// \return A future that resolves when the operation is complete.
    seastar::future<> change_host_filter(host_filter filter);

    const host_filter& get_host_filter() const noexcept;

    /// \brief Check if a hint may be generated to a given host.
    /// \param host_id destination host
    /// \return true if we should generate the hint to the given host if it becomes unavailable
    bool can_hint_for(const locator::host_id& host_id) const noexcept;

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
    bool too_many_in_flight_hints_for(const locator::host_id& host_id) const noexcept;

    /// \brief Check if DC \param ep belongs to is "hintable"
    /// \param ep End point identificator
    /// \return TRUE if hints are allowed to be generated to \param ep.
    bool check_dc_for(const locator::host_id& host_id) const noexcept;

    /// \brief Checks if hints are disabled for all endpoints
    /// \return TRUE if hints are disabled.
    bool is_disabled_for_all() const noexcept;

    /// \return Size of mutations of hints in-flight (to the disk) at the moment.
    uint64_t size_of_hints_in_progress() const noexcept;

    /// \brief Get the number of in-flight (to the disk) hints to a given host.
    /// \param host_id Host identifier
    /// \return Number of hints in-flight to \param host_id.
    uint64_t hints_in_progress_for(const locator::host_id& host_id) const noexcept;

    // ???
    void add_host_with_pending_hints(const locator::host_id& host_id);
    // ???
    void clear_hosts_with_pending_hints();
    // ???
    bool has_host_with_pending_hints(const locator::host_id& host_id) const;

    size_t managed_hosts_count() const noexcept;

    const std::filesystem::path& hints_dir() const;
    dev_t hints_dir_device_id() const;

    void allow_hints();
    void forbid_hints();
    void forbid_hints_for_hosts_with_pending_hints();
    void allow_replaying() noexcept;

    sync_point::shard_rps calculate_current_sync_point(const std::span<locator::host_id> target_hosts) const;
    
    seastar::future<> wait_for_sync_point(seastar::abort_source& as,
            const sync_point::shard_rps& rps);
    
    // TODO: Can this take a reference? Or should we take the ID by value?
    void drain_for(const locator::host_id& host_id);
    
    // ???
    void update_backlog(size_t backlog, size_t max_backlog);

    bool stopping() const noexcept;
    bool started() const noexcept;
    bool replay_allowed() const noexcept;
    bool draining_all() const noexcept;

    template<typename Func>
    decltype(auto) host_lock_invoke(const locator::host_id& host_id, Func&& f) {
        seastar::lw_shared_ptr<seastar::shared_mutex> mutex_ptr = get_host_hint_file_mutex(host_id);
        // TODO: As far as I remember, Scylla has its own dedicated function
        // to assert something. Look for it. For the time being,
        // `assert` is fine.
        //
        // The pointer should be valid. If the host of the specified
        // host ID is not managed by this object, an exception should
        // have been thrown by now.
        assert(mutex_ptr);
        co_return co_await seastar::with_lock(*mutex_ptr, std::forward<Func>(f));
    }

private:
    seastar::lw_shared_ptr<seastar::shared_mutex> get_host_hint_file_mutex(const locator::host_id& host_id);
};

seastar::future<> rebalance(seastar::sstring hints_directory);

} // namespace db::hints
