/*
 * Modified by ScyllaDB
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "db/hints/manager.hh"

// Seastar features.
#include <seastar/core/abort_source.hh>
#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file-types.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/print.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_mutex.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/thread.hh>
#include <seastar/coroutine/parallel_for_each.hh>

// Scylla includes.
#include "db/commitlog/commitlog.hh"
#include "db/commitlog/commitlog_entry.hh"
#include "db/commitlog/replay_position.hh"
#include "db/hints/resource_manager.hh"
#include "db/timeout_clock.hh"
#include "dht/i_partitioner.hh"
#include "gms/inet_address.hh"
#include "locator/token_metadata.hh"
#include "locator/topology.hh"
#include "mutation/frozen_mutation.hh"
#include "mutation/mutation.hh"
#include "schema/schema.hh"
#include "schema/schema_fwd.hh"
#include "service/storage_proxy.hh"
#include "utils/directories.hh"
#include "utils/disk-error-handler.hh"
#include "utils/div_ceil.hh"
#include "utils/error_injection.hh"
#include "utils/fb_utilities.hh"
#include "utils/lister.hh"
#include "utils/runtime.hh"
#include "converting_mutation_partition_applier.hh"
#include "inet_address_vectors.hh"
#include "log.hh"

// STD.
#include <chrono>
#include <exception>
#include <numeric>
#include <optional>
#include <stdexcept>
#include <unordered_map>
#include <utility>

namespace db::hints {

// Auxiliary stuff.
namespace {

// Constants.
// TODO: Change this to constexpr once the minimum supported versions of libc++ and libstdc++
//       have implemented constexpr std::string.
const std::string FILENAME_PREFIX{"HintsLog" + commitlog::descriptor::SEPARATOR};
constexpr std::chrono::seconds HINT_FILE_WRITE_TIMEOUT = std::chrono::seconds{2};


logging::logger manager_logger{"hints_manager"};

std::chrono::seconds hints_flush_period = std::chrono::seconds{10};

class no_column_mapping : public std::out_of_range {
public:
    no_column_mapping(const table_schema_version& id)
        : std::out_of_range{seastar::format("column mapping for CF schema version {} is missing", id)}
    {}
};

// We access the token so often in this file that providing a safe getter brings quite a lot of convenience.
//
// The main reason behind this is that some of `manager`'s methods are called even before it starts,
// and we might be trying to dereference a null pointer. This function is a way to prevent that.
//
// For more context, check `service::storage_proxy::cannot_hint`, which is one situation when `manager` may be used
// even before starting.
inline locator::token_metadata_ptr get_token_metadata_ptr(seastar::shared_ptr<service::storage_proxy> proxy) noexcept {
    // The manager will only store a null pointer at the beginning of Scylla's routine.
    if (!proxy) [[unlikely]] {
        return nullptr;
    } else {
        return proxy->get_token_metadata_ptr();
    }
}

/// \brief Get the last modification time stamp for a given file.
/// \param fname File name
/// \return The last modification time stamp for \param fname.
seastar::future<::timespec> get_last_file_modification(const seastar::sstring& fname) {
    file f = co_await seastar::open_file_dma(fname, seastar::open_flags::ro);
    const auto st = co_await f.stat();
    co_return st.st_mtim;
}

} // anonymous namespace



/////////////////////////////
/////////////////////////////
//.........................//
//..directory initializer..//
//.........................//
/////////////////////////////
/////////////////////////////

class directory_initializer::impl {
private:
    enum class state {
        uninitialized = 0,
        created_and_validated = 1,
        rebalanced = 2
    };

private:
    utils::directories& _dirs;
    seastar::sstring _hints_directory;
    state _state = state::uninitialized;
    seastar::named_semaphore _lock = {1, seastar::named_semaphore_exception_factory{"hints directory initialization lock"}};

public:
    impl(utils::directories& dirs, seastar::sstring hints_directory)
        : _dirs{dirs}
        , _hints_directory{std::move(hints_directory)}
    {}

public:
    seastar::future<> ensure_created_and_verified() {
        if (_state > state::uninitialized) {
            co_return;
        }

        const auto units = co_await seastar::get_units(_lock, 1);
        
        utils::directories::set dir_set;
        dir_set.add_sharded(_hints_directory);

        manager_logger.debug("Creating and validating hint directories: {}", _hints_directory);
        co_await _dirs.create_and_verify(std::move(dir_set));

        _state = state::created_and_validated;
    }

    seastar::future<> ensure_rebalanced() {
        if (_state < state::created_and_validated) {
            throw std::logic_error{"hints directory needs to be created and validated before rebalancing"};
        }

        if (_state > state::created_and_validated) {
            co_return;
        }

        const auto units = co_await seastar::get_units(_lock, 1);

        manager_logger.debug("Rebalancing hints in {}", _hints_directory);
        co_await rebalance(_hints_directory);

        _state = state::rebalanced;
    }
};

directory_initializer::directory_initializer(std::shared_ptr<directory_initializer::impl> impl)
    : _impl{std::move(impl)}
{}

directory_initializer::~directory_initializer() {}

seastar::future<directory_initializer> directory_initializer::make(utils::directories& dirs, seastar::sstring hints_directory) {
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
        return impl->ensure_created_and_verified().then([impl] {/* extend the lifetime? */});
    });
}

seastar::future<> directory_initializer::ensure_rebalanced() {
    if (!_impl) {
        return seastar::make_ready_future<>();
    }

    return smp::submit_to(0, [impl = _impl] () mutable {
        return impl->ensure_rebalanced().then([impl] {/* extend the lifetime? */});
    });
}


/////////////////////////////////////////////
/////////////////////////////////////////////
/////////////////////////////////////////////
/////////////////////////////////////////////
/////////////////////////////////////////////
/////////////////////////////////////////////


namespace {

namespace fs = std::filesystem;

using manager_impl = internal::manager_impl;
using drain = seastar::bool_class<struct drain_tag>;

class host_manager;

struct send_file_ctx {
public:
    template<typename Key, typename Value>
    using map_type = std::unordered_map<Key, Value>;
    template<typename Value>
    using set_type = std::set<Value>;

public:
    map_type<table_schema_version, column_mapping>& schema_ver_to_column_mapping;
    seastar::gate file_send_gate;

    std::optional<replay_position> first_failed_rp;
    std::optional<replay_position> last_succeeded_rp;
    set_type<replay_position> in_progress_rps;

    bool segment_replay_failed = false;
    
public:
    send_file_ctx(map_type<table_schema_version, column_mapping>& last_schema_ver_to_column_mapping) noexcept
        : schema_ver_to_column_mapping{last_schema_ver_to_column_mapping}
    {}

    void mark_hint_as_in_progress(replay_position rp) {
        in_progress_rps.insert(std::move(rp));
    }
    void on_hint_send_success(const replay_position& rp) {
        in_progress_rps.erase(rp);
        if (!last_succeeded_rp || *last_succeeded_rp < rp) {
            last_succeeded_rp = rp;
        }
    }
    void on_hint_send_failure(const replay_position& rp) {
        in_progress_rps.erase(rp);
        segment_replay_failed = true;
        if (!first_failed_rp || rp < *first_failed_rp) {
            first_failed_rp = rp;
        }
    }

    replay_position get_replayed_bound() const noexcept {
        // We are sure that all hints have been sent _below_ the position which is the minimum of the following:
        // - Position of the first hint that failed to be sent in this replay (first_failed_rp),
        // - Position of the last hint which was successfully sent (last_succeeded_rp, inclusive bound),
        // - Position of the lowest hint which is being currently sent (in_progress_rps.begin()).

        replay_position rp;
        if (first_failed_rp) {
            rp = *first_failed_rp;
        } else if (last_succeeded_rp) {
            // It is always true that `first_failed_rp` <= `last_succeeded_rp`, so no need to compare.
            rp = *last_succeeded_rp;
            // We replayed _up to_ `last_attempted_rp`, so the bound is not strict; we can increase `pos` by one.
            rp.pos++;
        }

        if (!in_progress_rps.empty() && *in_progress_rps.begin() < rp) {
            rp = *in_progress_rps.begin();
        }

        return rp;
    }
};

class host_sender {
private:
    using clock_type = seastar::lowres_clock;
    using timepoint_type = typename clock_type::time_point;
    using duration_type = typename clock_type::duration;

    static_assert(noexcept(clock_type::now()), "clock_type::now() must be noexcept");

    enum class state {
        stopping,       // stop() has been called
        host_left_ring, // The destination is not part of the ring anymore.
                        // That usually means that it has been decommissioned.
        draining        // Try to send everything and ignore errors.
    };

    using state_set = enum_set<super_enum<state,
            state::stopping,
            state::host_left_ring,
            state::draining>>;
    
    template<typename Key, typename Value>
    using map_type = typename send_file_ctx::map_type<Key, Value>;

private:
    // TODO: Check if using `std::list` wasn't necessary because of
    //       guarantess about iterators. Who knows if we don't rely
    //       on them being valid even if something with the list
    //       happens in the meantime.
    //
    // TODO: Maybe `seastar::circular_buffer` could also be nice here?
    //       Do we need to allocate in chunks? Read the comment in
    //       the file corresponding to `seastar::chunked_fifo`.
    //       There is a comparison of the data structures.
    seastar::chunked_fifo<seastar::sstring> _segments_to_replay{};
    seastar::chunked_fifo<seastar::sstring> _foreign_segments_to_replay{};

    replay_position _last_not_complete_rp{};
    replay_position _sent_upper_bound_rp{};

    map_type<table_schema_version, column_mapping> _last_schema_ver_to_column_mapping{};

    state_set _state{};
    seastar::future<> _stopped = seastar::make_ready_future<>();
    seastar::abort_source _stop_as{};

    timepoint_type _next_flush_tp{};
    timepoint_type _next_send_retry_tp{};

    const locator::host_id _host_id;
    host_manager& _host_manager;

    seastar::scheduling_group _hints_cpu_sched_group;
    // The value is equal to nullptr iff the promise has already been resolved.
    std::multimap<replay_position, seastar::lw_shared_ptr<seastar::promise<>>> _reply_waiters{};

public:
    host_sender(host_manager& parent, seastar::scheduling_group sched_group) noexcept
        : _host_id{parent.host_id()}
        , _host_manager{parent}
        , _hints_cpu_sched_group{sched_group}
    {}

    host_sender(host_sender&&) = default;
    host_sender(const host_sender&) = delete;

    host_sender(host_sender&& other, host_manager& parent)
        : _segments_to_replay{std::move(other._segments_to_replay)}
        , _foreign_segments_to_replay{std::move(other._foreign_segments_to_replay)}
        , _last_not_complete_rp{std::move(other._last_not_complete_rp)}
        , _sent_upper_bound_rp{std::move(other._sent_upper_bound_rp)}
        , _last_schema_ver_to_column_mapping{std::move(other._last_schema_ver_to_column_mapping)}
        , _state{std::move(other._state)}
        , _stopped{std::move(other._stopped)}
        , _stop_as{std::move(other._stop_as)}
        , _next_flush_tp{std::move(other._next_flush_tp)}
        , _next_send_retry_tp{std::move(other._next_send_retry_tp)}
        , _host_id{std::move(other._host_id)}
        // The only non-trivial assignment in this constructor.
        , _host_manager{parent}
        , _hints_cpu_sched_group{std::move(other._hints_cpu_sched_group)}
        , _reply_waiters{std::move(other._reply_waiters)}
    {}

    ~host_sender() noexcept {
        assert(stopped());
    }

public:
    void start();
    seastar::future<> stop(drain should_drain) noexcept;

    void add_segment(seastar::sstring segment_name);
    void add_foreign_segment(seastar::sstring segment_name);
    bool have_segments() const noexcept {
        return !_segments_to_replay.empty() || !_foreign_segments_to_replay.empty();
    }

    // TODO: Can be taken by reference?
    void rewind_sent_replay_position_to(replay_position rp);
    // TODO: Ditto.
    seastar::future<> wait_until_hints_are_replayed_up_to(seastar::abort_source& as,
            replay_position up_to_rp);

};


using node_to_hint_store_factory_type = utils::loading_shared_values<locator::host_id, commitlog>;
using hints_store_ptr = typename node_to_hint_store_factory_type::entry_ptr;


class host_manager {
private:
    enum class state {
        can_hint,   // Hinting is allowed (used by the space_watchdog).
        stopping,   // Stopping is in progress -- stop() has been called.
        stopped     // stop() has finished.
    };

    using state_set = enum_set<super_enum<state,
            state::can_hint,
            state::stopping,
            state::stopped>>;

private:
    locator::host_id _host_id;
    seastar::lw_shared_ptr<seastar::shared_mutex> _file_mutex_ptr;
    manager_impl& _shard_manager;
    host_sender _sender;
    state_set _state;

    const fs::path _hints_dir;
    seastar::gate _store_gate;
    hints_store_ptr _hints_store_anchor;

    uint64_t _hints_in_progress = 0;
    replay_position _last_written_rp;

public:
    host_manager(const locator::host_id& host_id, manager_impl& shard_manager)
        : _host_id{host_id}
        , _file_mutex_ptr{seastar::make_lw_shared(seastar::shared_mutex{})}
        , _shard_manager{shard_manager}
        , _sender{*this, _shard_manager.local_db().get_streaming_scheduling_group()}
        , _state{state_set::of<state::stopped>()}
        , _hints_dir{_shard_manager.hints_dir() / seastar::format("{}", _host_id).c_str()}
        // Approximate the position of the last written hint by using the same formula
        // as for segment id calculation in commitlog.
        // TODO: Should this logic be deduplicated with what is in the commitlog?
        , _last_written_rp{seastar::this_shard_id(),
                std::chrono::duration_cast<std::chrono::milliseconds>(
                        runtime::get_boot_time().time_since_epoch()).count()}
    {}

    host_manager(host_manager&& other)
        : _host_id{std::move(other._host_id)}
        , _file_mutex_ptr{std::move(other._file_mutex_ptr)}
        , _shard_manager{other._shard_manager}
        // The only non-trivial assignment in this constructor.
        , _sender{std::move(other._sender), *this}
        , _state{std::move(other._state)}
        , _hints_dir{std::move(other._hints_dir)}
        , _last_written_rp{std::move(other._last_written_rp)}
    {}

    ~host_manager() noexcept {
        assert(stopped());
    }

public:
    void start();
    // ...
};

} // anonymous namespace

namespace internal {

class manager_impl {
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

private:
    static constexpr uint64_t max_size_of_hints_in_progress =
            manager::max_size_of_hints_in_progress;

private:
    state_set _state;
    const fs::path _hints_dir;
    dev
};

} // namespace internal



} // namespace db::hints
