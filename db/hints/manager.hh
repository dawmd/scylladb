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
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace db::hints {

using node_to_hint_store_factory_type = utils::loading_shared_values<locator::host_id, commitlog>;
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
        return {nullptr};
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
            using time_duration_type = typename clock_type::duration;
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

                std::optional<replay_position> first_failed_rp;
                std::optional<replay_position> last_succeeded_rp;
                std::set<replay_position> in_progress_rps;

                bool segment_replay_failed = false;

            public:
                send_one_file_ctx(std::unordered_map<table_schema_version, column_mapping>& last_schema_ver_to_column_mapping)
                    : schema_ver_to_column_mapping{last_schema_ver_to_column_mapping}
                {}

                void mark_hint_as_in_progress(replay_position rp);
                void on_hint_send_success(replay_position rp) noexcept;
                void on_hint_send_failure(replay_position rp) noexcept;

                replay_position get_replayed_bound() const noexcept;
            };

        private:
            std::list<seastar::sstring> _segments_to_replay;
            std::list<seastar::sstring> _foreign_segments_to_replay;

            replay_position _last_not_complete_rp;
            replay_position _sent_upper_bound_rp;

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
            std::multimap<replay_position, seastar::lw_shared_ptr<std::optional<seastar::promise<>>>> _replay_waiters;

        public:
            sender(host_hint_manager& parent, service::storage_proxy& local_storage_proxy,
                    replica::database& local_db, gms::gossiper& local_gossiper) noexcept;

            /// \brief A constructor that should be called from the copy/move-constructor of host_hint_manager.
            ///
            /// Make sure to properly reassign the references - especially to the \param parent and its internals.
            ///
            /// \param other the "sender" instance to copy from
            /// \param parent the parent object for this "sender" instance
            sender(const sender& other, host_hint_manager& parent) noexcept;
            
            ~sender();

        public:
            /// \brief Start sending hints.
            ///
            /// Flush hints aggregated to far to the storage every hints_flush_period.
            /// If the _segments_to_replay is not empty sending send all hints we have.
            /// TODO: Make sure the wording above is fine, i.e. it can be understood unambiguously.
            ///
            /// Sending is stopped when stop() is called.
            void start();

            /// \brief Stop the sender - make sure all background sending is complete.
            /// \param should_drain if is drain::yes - drain all pending hints
            seastar::future<> stop(drain should_drain) noexcept;

            /// \brief Add a new segment ready for sending.
            void add_segment(seastar::sstring seg_name);

            /// \brief Add a new segment originating from another shard and ready for sending.
            void add_foreign_segment(seastar::sstring seg_name);

            /// \brief Check if there are still unsent segments.
            /// \return TRUE if there are still unsent segments.
            bool have_segments() const noexcept {
                return !_segments_to_replay.empty() || !_foreign_segments_to_replay.empty();
            }

            /// \brief Set the sent_upper_bound_rp marker to indicate that the hints were replayed _up to_ the given position.
            void rewind_sent_replay_position_to(replay_position rp);

            /// \brief Wait until hints are replayed up to the given replay position or until the given abort source is triggered.
            seastar::future<> wait_until_hints_are_replayed_up_to(seastar::abort_source& as, replay_position up_to_rp);
        
        private:
            /// \brief Get the name of the current segment that should be sent.
            ///
            /// If there are no segments to be sent, nullptr will be returned.
            const seastar::sstring* name_of_current_segment() const noexcept;

            /// \brief Remove the current segment from the queue.
            void pop_current_segment() noexcept;

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

            /// \brief Check if we can still send hints.
            /// \return TRUE if the destination node is either ALIVE or has left the ring (e.g. after decommission or removenode).
            bool can_send() noexcept;

            /// \brief Get a reference to the column_mapping object for a given frozen mutation.
            /// \param ctx_ptr pointer to the send context
            /// \param fm Frozen mutation object
            /// \param hr hint entry reader object
            /// \return
            const column_mapping& get_column_mapping(seastar::lw_shared_ptr<send_one_file_ctx> ctx_ptr,
                    const frozen_mutation& fm, const hint_entry_reader& hr);

            /// \brief Restore a mutation object from the hints file entry.
            /// \param ctx_ptr pointer to the send context
            /// \param buf hints file entry
            /// \return The mutation object representing the original mutation stored in the hints file.
            frozen_mutation_and_schema get_mutation(seastar::lw_shared_ptr<send_one_file_ctx> ctx_ptr,
                    fragmented_temporary_buffer& buf);
            
            /// \brief Send one mutation out.
            ///
            /// \param m mutation to send
            /// \return future that resolves when the mutation sending processing is complete.
            seastar::future<> send_one_mutation(frozen_mutation_and_schema m);

            /// \brief Perform a single mutation send atempt.
            ///
            /// If the original destination endpoint is still a replica for the given mutation,
            /// send the mutation directly to it. Otherwise execute the mutation "from scratch" with CL=ALL.
            ///
            /// \param m mutation to send
            /// \param natural_endpoints current replicas for the given mutation
            /// \return future that resolves when the operation is complete
            seastar::future<> do_send_one_mutation(frozen_mutation_and_schema m,
                    const inet_address_vector_replica_set& natural_endpoints) noexcept;

            /// \brief Try to send one hint read from the file.
            ///  - Limit the maximum memory size of hints "in the air" and the maximum total number of hints "in the air".
            ///  - Discard the hints that are older than the grace seconds value of the corresponding table.
            ///
            /// If sending fails, we are going to set the state::segment_replay_failed in the _state,
            /// and _first_failed_rp will be updated to min(_first_failed_rp, \ref rp).
            ///
            /// \param ctx_ptr shared pointer to the file sending context
            /// \param buf buffer representing the hint
            /// \param rp replay position of this hint in the file (see commitlog for more details on "replay position")
            /// \param secs_since_file_mod last modification time stamp (in seconds since Epoch) of the current hints file
            /// \param fname name of the hints file this hint was read from
            /// \return future that resolves when next hint may be sent
            seastar::future<> send_one_hint(seastar::lw_shared_ptr<send_one_file_ctx> ctx_ptr, fragmented_temporary_buffer buf,
                    replay_position rp, gc_clock::duration secs_since_file_mod, const seastar::sstring& fname);
            
            /// \brief Send all hint from a single file, and delete it after it has been successfully sent.
            ///
            /// Send all hints from the given file. If we fail to send the current segment,
            /// we will continue sending in the next iteration from where we finish this one.
            ///
            /// \param fname file to send
            /// \return TRUE if file has been successfully sent
            bool send_one_file(const seastar::sstring& name);
            
            void send_hints_maybe() noexcept;

            /// \brief Flush all pending hints to storage if hints_flush_period has passed since the last flush event.
            /// \return Ready, never exceptional, future when operation is complete.
            seastar::future<> flush_maybe() noexcept;

            /// \brief Notify those replay waiters for which the target replay position was reached.
            void notify_replay_waiters() noexcept;

            /// \brief Dismiss ALL current replay waiters with an exception.
            void dismiss_replay_waiters() noexcept;

            /// \brief Return the amount of time we want to sleep after the current iteration.
            /// \return The time till the soonest event: flushing or re-sending.
            time_duration_type next_sleep_duration() const;

            stats& shard_stats() {
                return _shard_manager._stats;
            }
        };
    
    private:
        locator::host_id _host_id;
        seastar::lw_shared_ptr<seastar::shared_mutex> _file_update_mutex_ptr;
        manager& _shard_manager;
        sender _sender;
        hint_manager_state_set _state;

        const fs::path _hints_dir;
        seastar::gate _store_gate;
        hints_store_ptr _hints_store_anchor;

        uint64_t _hints_in_progress = 0;
        replay_position _last_written_rp;
    
    public:
        host_hint_manager(const locator::host_id& host_id, manager& shard_manager);
        host_hint_manager(host_hint_manager&&);
        ~host_hint_manager();

    public:
        /// \brief Start the timer.
        void start();

        /// \brief Get the corresponding hints_store object. Create it if needed.
        /// \note Must be called under the \ref _file_update_mutex.
        /// \return The corresponding hints_store object.

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

        seastar::future<hints_store_ptr> get_or_load();

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

        /// \brief Waits until hints are replayed up to a given replay position, or given abort source is triggered.
        seastar::future<> wait_until_hints_are_replayed_up_to(seastar::abort_source& as, replay_position up_to_rp) {
            return _sender.wait_until_hints_are_replayed_up_to(as, up_to_rp);
        }

        /// \brief Returns replay position of the most recently written hint.
        ///
        /// If there weren't any hints written during this endpoint manager's lifetime, a zero replay_position is returned.
        replay_position last_written_replay_position() const {
            return _last_written_rp;
        }

        /// \return Number of in-flight (towards the file) hints.
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

        const fs::path& hints_dir() const noexcept {
            return _hints_dir;
        }

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
        friend inline auto with_file_update_mutex(host_hint_manager& hhman, Func&& func) {
            return seastar::with_lock(*hhman._file_update_mutex_ptr, std::forward<Func>(func))
                    .finally([lock_ptr = hhman._file_update_mutex_ptr] {});
        }

    private:
        /// \brief Creates a new hints store object.
        ///
        /// - Creates a hints store directory if doesn't exist: <shard_hints_dir>/<host_id>
        /// - Creates a store object.
        /// - Populate _segments_to_replay if it's empty.
        ///
        /// \return A new hints store object.
        seastar::future<commitlog> add_store() noexcept;

        /// \brief Flushes all hints written so far to the disk.
        ///  - Repopulates the _segments_to_replay list if needed.
        ///
        /// \return Ready future when the procedure above completes.
        seastar::future<> flush_current_hints() noexcept;
        
        seastar::shared_mutex& file_update_mutex() noexcept {
            return *_file_update_mutex_ptr;
        }
        resource_manager& shard_resource_manager() {
            return _shard_manager._resource_manager;
        }
        stats& shard_stats() {
            return _shard_manager._stats;
        }
    };

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
    ~manager() noexcept;

public:
    seastar::future<> start(seastar::shared_ptr<service::storage_proxy> proxy_ptr, seastar::shared_ptr<gms::gossiper> gossiper_ptr);
    seastar::future<> stop();

    void register_metrics(const seastar::sstring& group_name);
    
    bool store_hint(const locator::host_id& host_id, schema_ptr s, seastar::lw_shared_ptr<const frozen_mutation> fm,
            tracing::trace_state_ptr tr_state) noexcept;
    
    /// \brief Changes the host_filter currently used, stopping and starting ep_managers relevant to the new host_filter.
    /// \param filter the new host_filter
    /// \return A future that resolves when the operation is complete.
    seastar::future<> change_host_filter(host_filter filter);

    const host_filter& get_host_filter() const noexcept {
        return _host_filter;
    }

    /// \brief Check if a hint may be generated to the give end point
    /// \param ep end point to check
    /// \return true if we should generate the hint to the given end point if it becomes unavailable
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
    bool is_disabled_for_all() const noexcept {
        return _host_filter.is_disabled_for_all();
    }

    /// \return Size of mutations of hints in-flight (to the disk) at the moment.
    uint64_t size_of_hints_in_progress() const noexcept {
        return _stats.size_of_hints_in_progress;
    }

    /// \brief Get the number of in-flight (to the disk) hints to a given host.
    /// \param host_id Host identifier
    /// \return Number of hints in-flight to \param host_id.
    uint64_t hints_in_progress_for(const locator::host_id& host_id) const noexcept {
        auto it = _host_managers.find(host_id);
        return it != _host_managers.end()
                ? it->second.hints_in_progress()
                : 0;
    }

    void add_host_with_pending_hints(const locator::host_id& host_id) {
        _hosts_with_pending_hints.insert(host_id);
    }
    void clear_hosts_with_pending_hints() {
        _hosts_with_pending_hints.clear();
        _hosts_with_pending_hints.reserve(_host_managers.size());
    }
    bool has_host_with_pending_hints(const locator::host_id& host_id) const {
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

    /// \brief Return a set of replay positions for hint queues towards hosts from the `target_hosts`.
    sync_point::shard_rps calculate_current_sync_point(const std::vector<locator::host_id>& target_hosts) const;
    
    /// \brief Wait until hint replay reaches replay positions described in `rps`.
    seastar::future<> wait_for_sync_point(seastar::abort_source& as, const sync_point::shard_rps& rps);

    /// \brief Create an object which aids in hints directory initialization.
    ///
    /// This object can be safely copied and used from any shard.
    ///
    /// \arg dirs The utils::directories object, used to create and lock hints directories
    /// \arg hints_directory The directory with hints which should be initialized
    directory_initializer make_directory_initializer(utils::directories& dirs, fs::path hints_directory);

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
    static seastar::future<> rebalance(seastar::sstring hints_directory);

    /// \brief Initiate the draining when we detect that the node has left the cluster.
    ///
    /// If the node that has left is the current node - drains all pending hints to all nodes.
    /// Otherwise drains hints to the node that has left.
    ///
    /// In both cases - removes the corresponding hints' directories after all hints have been drained and erases the
    /// corresponding end_point_hints_manager objects.
    ///
    /// \param endpoint node that left the cluster
    void drain_for(const locator::host_id& host_id);
    void update_backlog(size_t backlog, size_t max_backlog);

    bool stopping() const noexcept {
        return _state.contains(state::stopping);
    }
    bool started() const noexcept {
        return _state.contains(state::started);
    }
    bool replay_allowed() const noexcept {
        return _state.contains(state::replay_allowed);
    }
    bool draining_all() noexcept {
        return _state.contains(state::draining_all);
    }

    // TODO: Please, ditch these if possible...
    auto find_host_manager(const locator::host_id& host_id) noexcept {
        return _host_managers.find(host_id);
    }
    auto find_host_manager(const locator::host_id& host_id) const noexcept {
        return _host_managers.find(host_id);
    }
    auto host_managers_end() noexcept {
        return _host_managers.end();
    }
    auto host_managers_end() const noexcept {
        return _host_managers.end();
    }

private:
    seastar::future<> compute_hints_dir_device_id();

    host_hint_manager& get_host_manager(const locator::host_id& host_id);

    bool have_host_manager(const locator::host_id& host_id) const noexcept {
        return _host_managers.contains(host_id);
    }

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

    void set_stopping() noexcept {
        _state.set(state::stopping);
    }
    void set_started() noexcept {
        _state.set(state::started);
    }
    void set_draining_all() noexcept {
        _state.set(state::draining_all);
    }
};

} // namespace db::hints
