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
#include "service/storage_proxy.hh"
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
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace db::hints {

using node_to_hint_store_factory_type = utils::loading_shared_values<locator::host_id, db::commitlog>;
using hints_store_ptr = typename node_to_hint_store_factory_type::entry_ptr;
using hint_entry_reader = commitlog_entry_reader;

class directory_initializer {
private:
    class impl;
    std::shared_ptr<impl> _impl;

private:
    directory_initializer(std::shared_ptr<impl> impl);
public:
    ~directory_initializer();

public:
    static directory_initializer make_dummy() noexcept {
        return { nullptr };
    }

    static seastar::future<directory_initializer> make(utils::directories& dirs, seastar::sstring hints_directory);

    seastar::future<> ensure_created_and_verified();
    seastar::future<> ensure_rebalanced();
};


class manager {
private:
    struct stats {
        uint64_t size_of_hints_in_progress = 0;
        uint64_t written = 0;
        uint64_t errors = 0;
        uint64_t dropped = 0;
        uint64_t sent = 0;
        uint64_t discarded = 0;
        uint64_t corrupted_files = 0;
    };

    using drain = seastar::bool_class<struct drain_tag>;

    enum class state {
        started,        // Hinting is currently allowed (start() has finished).
        replay_allowed, // Replaying (hints sending) is allowed.
        draining_all,   // Hinting is NOT allowed -- all host hint managers are being stopped
                        // because this node is leaving the cluster.
        stopping        // Hinting is NOT allowed -- stopping is in progress (stop() has been called).
    };

    using state_set = enum_set<super_enum<state,
            state::started,
            state::replay_allowed,
            state::draining_all,
            state::stopping>>;
    
    using shard_id_type = unsigned;
    using host_hints_segment_map = std::unordered_map<shard_id_type, std::list<fs::path>>;
    using hints_segment_map = std::unordered_map<locator::host_id, host_hints_segment_map>;

    class host_hint_manager {
    private:
        enum class hint_manager_state {
            can_hint,   // Hinting is allowed (used by the space_watchdog).
            stopping,   // Stopping is in progress -- stop() has been called.
            stopped     // stop() has finished.
        };

        using hint_manager_state_set = enum_set<super_enum<hint_manager_state,
                hint_manager_state::can_hint,
                hint_manager_state::stopping,
                hint_manager_state::stopped>>;

        class sender {
        private:
            using clock_type = seastar::lowres_clock;
            using time_point_type = typename clock_type::time_point;
            static_assert(noexcept(clock_type::now()), "clock_type::now() must be noexcept");

            enum class sender_state {
                stopping,       // stop() has been called
                host_left_ring, // The destination is not part of the ring anymore.
                                // That usually means that it has been decommissioned.
                draining        // Try to send everything and ignore errors.
            };

            using sender_state_set = enum_set<super_enum<sender_state,
                    sender_state::stopping,
                    sender_state::host_left_ring,
                    sender_state::draining>>;
            
            struct send_one_file_ctx {
            public:
                std::unordered_map<table_schema_version, column_mapping>& schema_ver_to_column_mapping;
                seastar::gate file_send_gate;

                std::optional<db::replay_position> first_failed_rp;
                std::optional<db::replay_position> last_succeeded_rp;
                std::set<db::replay_position> in_progress_rps;

                bool segment_replay_failed = false;

            public:
                send_one_file_ctx(std::unordered_map<table_schema_version, column_mapping>& last_schema_ver_to_column_mapping)
                : schema_ver_to_column_mapping(last_schema_ver_to_column_mapping)
                {}

                void mark_hint_as_in_progress(db::replay_position rp);
                void on_hint_send_success(db::replay_position rp) noexcept;
                void on_hint_send_failure(db::replay_position rp) noexcept;

                db::replay_position get_replayed_bound() const noexcept;
            };

        private:
            std::list<seastar::sstring> _segments_to_replay;
            std::list<seastar::sstring> _foreign_segments_to_replay;

            db::replay_position _last_not_complete_rp;
            db::replay_position _sent_upper_bound_rp;

            std::unordered_map<table_schema_version, column_mapping> _last_schema_ver_to_column_mapping;

            sender_state_set _state;
            seastar::future<> _stopped;
            
            seastar::abort_source _stop_as;

            time_point_type _next_flush_tp;
            time_point_type _next_send_retry_tp;

            locator::host_id _host_id;
            
            host_hint_manager& _host_manager;
            manager& _shard_manager;
            resource_manager& _resource_manager;
            service::storage_proxy& _proxy;
            replica::database& _db;
            seastar::scheduling_group _hints_cpu_sched_group;
            gms::gossiper& _gossiper;

            seastar::shared_mutex& _file_update_mutex;
            std::multimap<db::replay_position, seastar::lw_shared_ptr<std::optional<promise<>>>> _replay_waiters;

        public:
            sender(host_hint_manager& parent, service::storage_proxy& local_storage_proxy,
                    replica::database& local_db, gms::gossiper& local_gossiper) noexcept;
            sender(const sender& other, host_hint_manager& parent) noexcept;
            
            ~sender();

        public:
            void start();
            seastar::future<> stop(drain should_drain) noexcept;
            void add_segment(seastar::sstring seg_name);
            void add_foreign_segment(seastar::sstring seg_name);
            bool have_segments() const noexcept {
                return !_segments_to_replay.empty() || !_foreign_segments_to_replay.empty();
            }
            void rewind_sent_replay_position_to(db::replay_position rp);
            seastar::future<> wait_until_hints_are_replayed_up_to(seastar::abort_source& as, db::replay_position up_to_rp);
        
        private:
            const seastar::sstring* name_of_current_segment() const noexcept;
            void pop_current_segment() noexcept;
            void send_hints_maybe() noexcept;

            void set_draining() noexcept {
                _state.set(sender_state::draining);
            }
            bool draining() const noexcept {
                return _state.contains(sender_state::draining);
            }
            void set_stopping() noexcept {
                _state.set(sender_state::stopping);
            }
            bool stopping() const noexcept {
                return _state.contains(sender_state::stopping);
            }

            bool replay_allowed() const noexcept {
                return _host_manager.replay_allowed();
            }

            seastar::future<> send_one_hint(seastar::lw_shared_ptr<send_one_file_ctx> ctx_ptr, fragmented_temporary_buffer buf,
                    db::replay_position rp, gc_clock::duration secs_since_file_mod, const seastar::sstring& fname);
            bool send_one_file(const seastar::sstring& name);
            bool can_send() noexcept;
            frozen_mutation_and_schema get_mutation(seastar::lw_shared_ptr<send_one_file_ctx> ctx_ptr,
                    fragmented_temporary_buffer& buf);
            const column_mapping& get_column_mapping(seastar::lw_shared_ptr<send_one_file_ctx> ctx_ptr,
                    const frozen_mutation& fm, const hint_entry_reader& hr);
            seastar::future<> do_send_one_mutation(frozen_mutation_and_schema m,
                    const inet_address_vector_replica_set& natural_endpoints) noexcept;
            seastar::future<> send_one_mutation(frozen_mutation_and_schema m);
            void notify_replay_waiters() noexcept;
            void dismiss_replay_waiters() noexcept;
            
            static seastar::future<::timespec> get_last_file_modification(const seastar::sstring& fname);

            stats& shard_stats() {
                return _shard_manager._stats;
            }

            seastar::future<> flush_maybe() noexcept;
            locator::host_id host_id() const noexcept {
                return _host_id;
            }

            typename clock_type::duration next_sleep_duration() const;
        };
    
    private:
        locator::host_id _host_id;
        sender _sender;
        hint_manager_state_set _state;

        const fs::path _hints_dir;
        manager& _shard_manager;
        seastar::gate _store_gate;
        hints_store_ptr _hints_store_anchor;

        seastar::lw_shared_ptr<seastar::shared_mutex> _file_update_mutex_ptr;
        seastar::shared_mutex& _file_update_mutex;

        uint64_t _hints_in_progress = 0;
        db::replay_position _last_written_rp;
    
    public:
        host_hint_manager(locator::host_id host_id, manager& shard_manager);
        host_hint_manager(host_hint_manager&&);
        ~host_hint_manager();

    public:
        locator::host_id host_id() const noexcept {
            return _host_id;
        }
        
        seastar::future<hints_store_ptr> get_or_load();
        bool store_hint(schema_ptr s, seastar::lw_shared_ptr<const frozen_mutation> fm,
                tracing::trace_state_ptr tr_state) noexcept;
        seastar::future<> populate_segments_to_replay();
        seastar::future<> stop(drain should_drain = drain::no) noexcept;
        void start();
        uint64_t hints_in_progress() const noexcept {
            return _hints_in_progress;
        }
        bool replay_allowed() const noexcept {
            return _shard_manager.replay_allowed();
        }

        bool can_hint() const noexcept {
            return _state.contains(hint_manager_state::can_hint);
        }
        void allow_hints() noexcept {
            _state.set(hint_manager_state::can_hint);
        }
        void forbid_hints() noexcept {
            _state.remove(hint_manager_state::can_hint);
        }
        void set_stopping() noexcept {
            _state.set(hint_manager_state::stopping);
        }
        bool stopping() const noexcept {
            return _state.contains(hint_manager_state::stopping);
        }
        void set_stopped() noexcept {
            _state.set(hint_manager_state::stopped);
        }
        void clear_stopped() noexcept {
            _state.remove(hint_manager_state::stopped);
        }
        bool stopped() const noexcept {
            return _state.contains(hint_manager_state::stopped);
        }

        db::replay_position last_written_replay_position() const {
            return _last_written_rp;
        }

        seastar::future<> wait_until_hints_are_replayed_up_to(seastar::abort_source& as, db::replay_position up_to_rp) {
            return _sender.wait_until_hints_are_replayed_up_to(as, up_to_rp);
        }
        template<typename Func>
        friend inline auto with_file_update_mutex(host_hint_manager& hhman, Func&& func) {
            return with_lock(*hhman._file_update_mutex_ptr, std::forward<Func>(func)).finally([lock_ptr = hhman._file_update_mutex_ptr] {});
        }

        const fs::path& hints_dir() const noexcept {
            return _hints_dir;
        }
    
    private:
        seastar::shared_mutex& file_update_mutex() noexcept {
            return _file_update_mutex;
        }

        seastar::future<db::commitlog> add_store() noexcept;
        seastar::future<> flush_current_hints() noexcept;
        
        stats& shard_stats() {
            return _shard_manager._stats;
        }
        resource_manager& shard_resource_manager() {
            return _shard_manager._resource_manager;
        }
    };

public:
    static const std::string FILENAME_PREFIX;
    static std::chrono::seconds hints_flush_period;
    static const std::chrono::seconds hint_file_write_timeout;

private:
    static constexpr uint64_t max_size_of_hints_in_progress = 10 * 1024 * 1024; // 10 MB
private:
    state_set _state;
    const fs::path _hints_dir;
    dev_t _hints_dir_device_id = 0;

    node_to_hint_store_factory_type _store_factory;
    host_filter _host_filter;
    seastar::shared_ptr<service::storage_proxy> _proxy_anchor;
    seastar::shared_ptr<gms::gossiper> _gossiper_anchor;
    int64_t _max_hint_window_us = 0;
    replica::database& _local_db;

    seastar::gate _draining_hosts_gate;

    resource_manager& _resource_manager;

    std::unordered_map<locator::host_id, host_hint_manager> _host_managers;
    stats _stats;
    seastar::metrics::metric_groups _metrics;
    std::unordered_set<locator::host_id> _hosts_with_pending_hints;
    seastar::named_semaphore _drain_lock = { 1, named_semaphore_exception_factory{ "drain lock" } };

public:
    manager(seastar::sstring hints_directory, host_filter filter, int64_t max_hint_window_ms,
            resource_manager& res_manager, seastar::sharded<replica::database>& db);
    manager(manager&&) = delete;
    virtual ~manager();

public:
    void register_metrics(const seastar::sstring& group_name);
    seastar::future<> start(seastar::shared_ptr<service::storage_proxy> proxy_ptr, seastar::shared_ptr<gms::gossiper> gossiper_ptr);
    seastar::future<> stop();
    
    bool store_hint(locator::host_id host_id, schema_ptr s, seastar::lw_shared_ptr<const frozen_mutation> fm,
            tracing::trace_state_ptr tr_state) noexcept;
    
    seastar::future<> change_host_filter(host_filter filter);
    const host_filter& get_host_filter() const noexcept {
        return _host_filter;
    }

    bool can_hint_for(locator::host_id host_id) const noexcept;
    bool too_many_in_flight_hints_for(locator::host_id host_id) const noexcept;
    bool check_dc_for(locator::host_id host_id) const noexcept;
    bool is_disabled_for_all() const noexcept {
        return _host_filter.is_disabled_for_all();
    }
    uint64_t size_of_hints_in_progress() const noexcept {
        return _stats.size_of_hints_in_progress;
    }
    uint64_t hints_in_progress_for(locator::host_id host_id) const noexcept {
        auto it = _host_managers.find(host_id);
        return it != _host_managers.end()
                ? it->second.hints_in_progress()
                : 0;
    }

    void add_host_with_pending_hints(locator::host_id host_id) {
        _hosts_with_pending_hints.insert(host_id);
    }
    void clear_hosts_with_pending_hints() {
        _hosts_with_pending_hints.clear();
        _hosts_with_pending_hints.reserve(_host_managers.size());
    }
    bool has_host_with_pending_hints(locator::host_id host_id) const {
        return _hosts_with_pending_hints.contains(host_id);
    }
    bool host_managers_size() const noexcept {
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

    sync_point::shard_rps calculate_current_sync_point(const std::vector<locator::host_id>& target_hosts) const;
    seastar::future<> wait_for_sync_point(seastar::abort_source& as, const sync_point::shard_rps& rps);

    directory_initializer make_directory_initializer(utils::directories& dirs, fs::path hints_directory);

    static seastar::future<> rebalance(seastar::sstring hints_directory);

private:
    seastar::future<> compute_hints_dir_device_id();

    static hints_segment_map get_current_hints_segments(const seastar::sstring& hints_directory);
    static void rebalance_segments_for(locator::host_id host_id, size_t segments_per_shard,
            const seastar::sstring& hints_directory, host_hints_segment_map& host_segments,
            std::list<fs::path>& segments_to_move);
    static void rebalance_segments(const seastar::sstring& hints_directory, hints_segment_map& segment_map);
    static void remove_irrelevant_shards_directories(const seastar::sstring& hints_directory);

    node_to_hint_store_factory_type& store_factory() noexcept {
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
    host_hint_manager& get_host_manager(locator::host_id host_id);
    bool have_host_manager(locator::host_id host_id) const noexcept;

public:
    void drain_for(locator::host_id host_id);

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
