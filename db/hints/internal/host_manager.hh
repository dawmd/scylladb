/*
 * Modified by ScyllaDB
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */
#pragma once

/// For now, this file is supposed to provide a "template header", i.e. it should
/// make refactoring of the module easier. As moving things around takes time
/// and `manager::end_point_hints_manager` is a pretty big data structure,
/// let's just paste its API here. The implementation will follow in the future.

// Seastar features.
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/shared_mutex.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/smp.hh>

// Scylla includes.
#include "db/commitlog/replay_position.hh"
#include "db/hints/internal/common.hh"
#include "db/hints/internal/hint_sender.hh"
#include "db/hints/internal/hint_storage.hh"
#include "db/hints/resource_manager.hh"
#include "utils/runtime.hh"
#include "enum_set.hh"

// STD.
#include <cassert>
#include <chrono>
#include <filesystem>
#include <memory>
#include <unordered_map>

namespace db::hints {

class manager;

namespace internal {

class host_manager {
// Public declarations.
public:
    // Make this public so that `hint_sender` can access it
    // and use the same data structure.
    template <typename Key, typename Value>
    using map_type = std::unordered_map<Key, Value>;

// Private declarations.
private:
    enum class state {
        can_hint,   // hinting is currently allowed (used by the space_watchdog)
        stopping,   // stopping is in progress (stop() method has been called)
        stopped     // stop() has completed
    };

    using state_set = enum_set<super_enum<state,
            state::can_hint,
            state::stopping,
            state::stopped>>;

// Fields.
private:
    // The identifier of the host this manager manages and sends hints to.
    host_id_type _host_id;
    state_set _state{};

    manager& _shard_manager;

    // This anchor is created in `get_or_load`.
    hint_store_ptr _hint_store_anchor = nullptr;
    seastar::gate _store_gate{};
    seastar::lw_shared_ptr<seastar::shared_mutex> _file_update_mutex_ptr;

    std::filesystem::path _hints_dir;
    
    uint64_t _hints_in_progress = 0;
    // TODO: Explain what exactly it means that it's the last written replay position.
    replay_position _last_written_rp;
    hint_sender _hint_sender;

public:
    host_manager(const host_id_type& key, manager& shard_manager);
    host_manager(host_manager&& other);

    ~host_manager() noexcept {
        assert(stopped());
    }

public:
    const host_id_type& host_id() const noexcept {
        return _host_id;
    }

    /// \brief Get the corresponding hints_store object. Create it if needed.
    /// \note Must be called under the \ref _file_update_mutex.
    /// \return The corresponding hints_store object.
    seastar::future<hint_store_ptr> get_or_load();

    /// \brief Store a single mutation hint.
    /// \param s column family descriptor
    /// \param fm frozen mutation object
    /// \param tr_state trace_state handle
    /// \return FALSE if hint is definitely not going to be stored
    bool store_hint(schema_ptr s, seastar::lw_shared_ptr<const frozen_mutation> fm,
            tracing::trace_state_ptr tr_state) noexcept;

    /// \brief Populates the _segments_to_replay list.
    ///  Populates the _segments_to_replay list with the names of the files in the <manager hints files directory> directory
    ///  in the order they should be sent out.
    ///
    /// \return Ready future when end point hints manager is initialized.
    seastar::future<> populate_segments_to_replay();

    /// \brief Waits till all writers complete and shuts down the hints store. Drains hints if needed.
    ///
    /// If "draining" is requested - sends all pending hints out.
    ///
    /// When hints are being drained we will not stop sending after a single hint sending has failed and will continue sending hints
    /// till the end of the current segment. After that we will remove the current segment and move to the next one till
    /// there isn't any segment left.
    ///
    /// \param should_drain is drain::yes - drain all pending hints
    /// \return Ready future when all operations are complete
    seastar::future<> stop(drain should_drain = drain::no) noexcept;

    /// \brief Start the timer.
    void start();

    /// \return Number of in-flight (towards the file) hints.
    uint64_t hints_in_progress() const noexcept {
        return _hints_in_progress;
    }

    bool replay_allowed() const noexcept {
        return _shard_manager.replay_allowed();
    }

    bool can_hint() const noexcept {
        return _state.contains(state::can_hint);
    }

    void allow_hints() noexcept {
        _state.set(state::can_hint);
    }

    void forbid_hints() noexcept {
        _state.remove(state::can_hint);
    }

    void set_stopping() noexcept {
        _state.set(state::stopping);
    }

    bool stopping() const noexcept {
        return _state.contains(state::stopping);
    }

    void set_stopped() noexcept {
        _state.set(state::stopped);
    }

    void clear_stopped() noexcept {
        _state.remove(state::stopped);
    }

    bool stopped() const noexcept {
        return _state.contains(state::stopped);
    }

    /// \brief Returns replay position of the most recently written hint.
    ///
    /// If there weren't any hints written during this endpoint manager's lifetime, a zero replay_position is returned.
    replay_position last_written_replay_position() const {
        return _last_written_rp;
    }

    /// \brief Waits until hints are replayed up to a given replay position, or given abort source is triggered.
    seastar::future<> wait_until_hints_are_replayed_up_to(seastar::abort_source& as,
            replay_position up_to_rp);

        
    seastar::lw_shared_ptr<seastar::shared_mutex> get_file_update_mutex_ptr() noexcept {
        return _file_update_mutex_ptr;
    }

    const std::filesystem::path& hints_dir() const noexcept {
        return _hints_dir;
    }

    seastar::shared_mutex& file_update_mutex() noexcept {
        return *_file_update_mutex_ptr;
    }

    /// \brief Flushes all hints written so far to the disk.
    ///  - Repopulates the _segments_to_replay list if needed.
    ///
    /// \return Ready future when the procedure above completes.
    seastar::future<> flush_current_hints() noexcept;

private:
    /// \brief Creates a new hints store object.
    ///
    /// - Creates a hints store directory if doesn't exist: <shard_hints_dir>/<ep_key>
    /// - Creates a store object.
    /// - Populate _segments_to_replay if it's empty.
    ///
    /// \return A new hints store object.
    seastar::future<commitlog> add_store() noexcept;

    hint_stats& shard_stats() {
        return _shard_manager._stats;
    }

    resource_manager& shard_resource_manager() {
        return _shard_manager._resource_manager;
    }
};

} // namespace internal
} // namespace db::hints
