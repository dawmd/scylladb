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

// Scylla includes.
#include "db/commitlog/replay_position.hh"
#include "db/hints/internal/common.hh"
#include "db/hints/internal/hint_sender.hh"
#include "db/hints/internal/hint_storage.hh"
#include "enum_set.hh"

// STD.
#include <filesystem>
#include <memory>
#include <unordered_map>

namespace db::hints {
namespace internal {

// For the time being, make this class a template to simplify development.
template <typename ShardManager>
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
    
    using sender = hint_sender<host_manager>;

// Fields.
private:
    // The identifier of the host this manager manages and sends hints to.
    host_id_type _host_id;
    state_set _state;

    ShardManager& _shard_manager;
    hint_store_ptr _hint_store_anchor;
    seastar::gate _store_gate;
    seastar::lw_shared_ptr<seastar::shared_mutex> _file_update_mutex_ptr;

    std::filesystem::path _hints_dir;
    
    uint64_t _hints_in_progress = 0;
    // TODO: Explain what exactly it means that it's the last written replay position.
    replay_position _last_written_rp;
    std::unique_ptr<sender> _hint_sender;

public:
    host_manager(const host_id_type& key, ShardManager& shard_manager);
    host_manager(host_manager&&);
    ~host_manager() noexcept;
};

} // namespace internal
} // namespace db::hints
