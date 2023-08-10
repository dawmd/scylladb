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
#include <seastar/core/coroutine.hh>
#include <seastar/core/file-types.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/print.hh>
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

        namespace sc = seastar::coroutine;
        co_await seastar::with_semaphore(_lock, 1, sc::lambda([this] () -> seastar::future<> {
            utils::directories::set dir_set;
            dir_set.add_sharded(_hints_directory);

            manager_logger.debug("Creating and validating hint directories: {}", _hints_directory);
            co_await _dirs.create_and_verify(std::move(dir_set));
            _state = state::created_and_validated;
        }));
    }

    seastar::future<> ensure_rebalanced() {
        if (_state < state::created_and_validated) {
            throw std::logic_error{"hints directory needs to be created and validated before rebalancing"};
        }

        if (_state > state::created_and_validated) {
            co_return;
        }

        namespace sc = seastar::coroutine;
        co_await seastar::with_semaphore(_lock, 1, sc::lambda([this] () -> seastar::future<> {
            manager_logger.debug("Rebalancing hints in {}", _hints_directory);
            co_await manager::rebalance(_hints_directory);
            _state = state::rebalanced;
        }));
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


/////////////////////////
/////////////////////////
//.....................//
//..send one file ctx..//
//.....................//
/////////////////////////
/////////////////////////

void manager::host_hint_manager::sender::send_one_file_ctx::mark_hint_as_in_progress(replay_position rp) {
    in_progress_rps.insert(rp);
}

void manager::host_hint_manager::sender::send_one_file_ctx::on_hint_send_success(replay_position rp) noexcept {
    in_progress_rps.erase(rp);
    if (!last_succeeded_rp || *last_succeeded_rp < rp) {
        last_succeeded_rp = rp;
    }
}

void manager::host_hint_manager::sender::send_one_file_ctx::on_hint_send_failure(replay_position rp) noexcept {
    in_progress_rps.erase(rp);
    segment_replay_failed = true;
    if (!first_failed_rp || rp < *first_failed_rp) {
        first_failed_rp = rp;
    }
}

replay_position manager::host_hint_manager::sender::send_one_file_ctx::get_replayed_bound() const noexcept {
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





////////////////////
////////////////////
//................//
//.....sender.....//
//................//
////////////////////
////////////////////


manager::host_hint_manager::sender::sender(host_hint_manager& parent, service::storage_proxy& local_storage_proxy,
        replica::database& local_db, gms::gossiper& local_gossiper) noexcept
    : _stopped{seastar::make_ready_future<>()}
    , _host_id{parent._host_id}
    , _host_manager{parent}
    , _shard_manager{_host_manager._shard_manager}
    , _resource_manager{_shard_manager._resource_manager}
    , _proxy{local_storage_proxy}
    , _db{local_db}
    , _hints_cpu_sched_group{_db.get_streaming_scheduling_group()}
    , _gossiper{local_gossiper}
    , _file_update_mutex{_host_manager.file_update_mutex()}
{}

manager::host_hint_manager::sender::sender(const sender& other, host_hint_manager& parent) noexcept
    : _stopped{seastar::make_ready_future<>()}
    , _host_id{parent._host_id}
    , _host_manager{parent}
    , _shard_manager{_host_manager._shard_manager}
    , _resource_manager{_shard_manager._resource_manager}
    , _proxy{other._proxy}
    , _db{other._db}
    , _hints_cpu_sched_group{other._hints_cpu_sched_group}
    , _gossiper{other._gossiper}
    , _file_update_mutex{_host_manager.file_update_mutex()}
{}

manager::host_hint_manager::sender::~sender() {
    dismiss_replay_waiters();
}

void manager::host_hint_manager::sender::start() {
    seastar::thread_attributes attr;
    attr.sched_group = _hints_cpu_sched_group;

    _stopped = seastar::async(std::move(attr), [this] {
        manager_logger.trace("host_manager({})::sender: started", _host_id);
        while (!stopping()) {
            try {
                flush_maybe().get();
                send_hints_maybe();

                // If we got here, that means that either there are no more hints to be sent, or we have failed
                // to send the hints we have. In either case, it makes sense to wait a little before continuing.
                sleep_abortable(next_sleep_duration(), _stop_as).get();
            } catch (const seastar::sleep_aborted&) {
                break;
            } catch (...) {
                manager_logger.trace("sender: got the exception: {}", std::current_exception());
            }
        }
    });
}

seastar::future<> manager::host_hint_manager::sender::stop(drain should_drain) noexcept {
    return seastar::async([this, should_drain] {
        set_stopping();
        _stop_as.request_abort();
        _stopped.get();

        if (should_drain == drain::yes) {
            // "Draining" is performed by a sequence of following calls:
            // set_draining() -> send_hints_maybe() -> flush_current_hints() -> send_hints_maybe()
            //
            // Before sender::stop() is called the storing path for this end point is blocked and no new hints
            // will be generated when this method is running.
            //
            // send_hints_maybe() in a "draining" mode is going to send all hints from segments in the
            // _segments_to_replay.
            //
            // Therefore after the first call for send_hints_maybe() the _segments_to_replay is going to become empty
            // and the following flush_current_hints() is going to store all in-memory hints to the disk and re-populate
            // the _segments_to_replay.
            //
            // The next call for send_hints_maybe() will send the last hints to the current end point and when it is
            // done there is going to be no more pending hints and the corresponding hints directory may be removed.
            manager_logger.trace("Draining for {}: start", _host_id);
            set_draining();
            send_hints_maybe();
            _host_manager.flush_current_hints().handle_exception([] (auto eptr) {
                manager_logger.error("Failed to flush pending hints: {}. Ignoring...", eptr);
            }).get();
            send_hints_maybe();
            manager_logger.trace("Draining for {}: end", _host_id);
        }
        manager_logger.trace("ep_manager({})::sender: exiting", _host_id);
    });
}

void manager::host_hint_manager::sender::add_segment(seastar::sstring seg_name) {
    _segments_to_replay.emplace_back(std::move(seg_name));
}

void manager::host_hint_manager::sender::add_foreign_segment(seastar::sstring seg_name) {
    _foreign_segments_to_replay.emplace_back(std::move(seg_name));
}

void manager::host_hint_manager::sender::rewind_sent_replay_position_to(replay_position rp) {
    _sent_upper_bound_rp = rp;
    notify_replay_waiters();
}

seastar::future<> manager::host_hint_manager::sender::wait_until_hints_are_replayed_up_to(seastar::abort_source &as,
        replay_position up_to_rp)
{
    manager_logger.debug("[{}] wait_until_hints_are_replayed_up_to(): entering with target {}", _host_id, up_to_rp);
    if (_foreign_segments_to_replay.empty() && up_to_rp < _sent_upper_bound_rp) {
        manager_logger.debug("[{}] wait_until_hints_are_replayed_up_to(): hints have already been replayed above the point ({} < {})",
                _host_id, up_to_rp, _sent_upper_bound_rp);
        return seastar::make_ready_future<>();
    }

    if (as.abort_requested()) {
        manager_logger.debug("[{}] wait_until_hints_are_replayed_up_to(): already aborted - stopping", _host_id);
        return seastar::make_exception_future(abort_requested_exception{});
    }

    auto ptr = seastar::make_lw_shared(std::make_optional(seastar::promise<>{}));
    auto it = _replay_waiters.emplace(up_to_rp, ptr);
    auto sub = as.subscribe([this, ptr, it] () noexcept {
        if (!ptr->has_value()) {
            // The promise has already been resolved by `notify_replay_waiters` and removed from the map.
            return;
        }
        manager_logger.debug("[{}] wait_until_hints_are_replayed_up_to(): abort requested - stopping", _host_id);
        _replay_waiters.erase(it);
        (**ptr).set_exception(abort_requested_exception{});
    });

    // When the future resolves, the host manager is not guaranteed to exist anymore,
    // so we can't capture `this`.
    return (**ptr).get_future().finally([sub = std::move(sub), host_id = _host_id] {
        manager_logger.debug("[{}] wait_until_hints_are_replayed_up_to(): returning after the future has been satisfied", host_id);
    });
}

const seastar::sstring* manager::host_hint_manager::sender::name_of_current_segment() const noexcept {
    // Foreign segments are replayed first.
    if (!_foreign_segments_to_replay.empty()) {
        return std::addressof(_foreign_segments_to_replay.front());
    }
    if (!_segments_to_replay.empty()) {
        return std::addressof(_segments_to_replay.front());
    }
    return nullptr;
}

void manager::host_hint_manager::sender::pop_current_segment() noexcept {
    if (!_foreign_segments_to_replay.empty()) {
        _foreign_segments_to_replay.pop_front();
    } else if (!_segments_to_replay.empty()) {
        _segments_to_replay.pop_front();
    }
}

bool manager::host_hint_manager::sender::can_send() noexcept {
    if (stopping() && !draining()) {
        return false;
    }

    try {
        const auto tm_ptr = _proxy.get_token_metadata_ptr();
        if (!tm_ptr) [[unlikely]] {
            return false;
        }

        const std::optional<gms::inet_address> ep = tm_ptr->get_endpoint_for_host_id(_host_id);
        if (!ep.has_value()) [[unlikely]] {
            return false;
        }

        const auto ep_state_ptr = _gossiper.get_endpoint_state_for_endpoint_ptr(ep.value());
        if (ep_state_ptr && ep_state_ptr->is_alive()) {
            _state.remove(sender_state::host_left_ring);
            return true;
        } else {
            if (!_state.contains(sender_state::host_left_ring)) {
                _state.set_if<sender_state::host_left_ring>(
                        !_shard_manager.local_db().get_token_metadata().is_normal_token_owner(ep.value()));
            }
            // Send out hints if the destination node is part of the ring
            // -- we will send to all new replicas in this case.
            return _state.contains(sender_state::host_left_ring);
        }
    } catch (...) {
        return false;
    }
}

const column_mapping& manager::host_hint_manager::sender::get_column_mapping(
        seastar::lw_shared_ptr<send_one_file_ctx> ctx_ptr, const frozen_mutation& fm, const hint_entry_reader& hr)
{
    auto cm_it = ctx_ptr->schema_ver_to_column_mapping.find(fm.schema_version());
    if (cm_it == ctx_ptr->schema_ver_to_column_mapping.end()) {
        if (!hr.get_column_mapping()) {
            throw no_column_mapping{fm.schema_version()};
        }

        manager_logger.debug("new schema version {}", fm.schema_version());
        cm_it = ctx_ptr->schema_ver_to_column_mapping.emplace(fm.schema_version(), *hr.get_column_mapping()).first;
    }
    return cm_it->second;
}

frozen_mutation_and_schema manager::host_hint_manager::sender::get_mutation(
        seastar::lw_shared_ptr<send_one_file_ctx> ctx_ptr, fragmented_temporary_buffer& buf)
{
    hint_entry_reader hr{buf};
    auto& fm = hr.mutation();
    auto& cm = get_column_mapping(ctx_ptr, fm, hr);
    auto schema = _db.find_schema(fm.column_family_id());

    if (schema->version() != fm.schema_version()) {
        mutation m{schema, fm.decorated_key(*schema)};
        converting_mutation_partition_applier v{cm, *schema, m.partition()};
        fm.partition().accept(cm, v);
        return {freeze(m), std::move(schema)};
    }
    return {std::move(hr).mutation(), std::move(schema)};
}

seastar::future<> manager::host_hint_manager::sender::send_one_mutation(frozen_mutation_and_schema m) {
    const auto& erm = _db.find_column_family(m.s).get_effective_replication_map();
    auto token = dht::get_token(*m.s, m.fm.key());
    inet_address_vector_replica_set natural_endpoints = erm->get_natural_endpoints(std::move(token));

    return do_send_one_mutation(std::move(m), natural_endpoints);
}

seastar::future<> manager::host_hint_manager::sender::do_send_one_mutation(frozen_mutation_and_schema m,
        const inet_address_vector_replica_set& natural_endpoints) noexcept
{
    return seastar::futurize_invoke([this, m = std::move(m), &natural_endpoints] () mutable {
        const std::optional<gms::inet_address> ep = _proxy.get_token_metadata_ptr()->get_endpoint_for_host_id(_host_id);

        // The fact that we send with CL::ALL in both cases below ensures that new hints are not going
        // to be generated as a result of hints sending.
        if (ep && std::ranges::find(natural_endpoints, ep) != natural_endpoints.end()) {
            manager_logger.trace("Sending directly to {}", _host_id);
            return _proxy.send_hint_to_endpoint(std::move(m), ep.value());
        } else {
            manager_logger.trace("Endpoints set has changed and {} is no longer a replica. Mutating from scratch...", _host_id);
            return _proxy.send_hint_to_all_replicas(std::move(m));
        }
    });
}

seastar::future<> manager::host_hint_manager::sender::send_one_hint(seastar::lw_shared_ptr<send_one_file_ctx> ctx_ptr,
        fragmented_temporary_buffer buf, replay_position rp, gc_clock::duration secs_since_file_mod,
        const seastar::sstring& fname)
{
    return _resource_manager.get_send_units_for(buf.size_bytes()).then(
            [this, secs_since_file_mod, &fname, buf = std::move(buf), rp, ctx_ptr] (auto units) mutable {
        ctx_ptr->mark_hint_as_in_progress(rp);

        // The future is waited on indirectly in `send_one_file()` (via `ctx_ptr->file_send_gate`).
        (void) seastar::with_gate(ctx_ptr->file_send_gate,
                [this, secs_since_file_mod, &fname, buf = std::move(buf), rp, ctx_ptr] () mutable -> seastar::future<> {
            try {
                auto m = this->get_mutation(ctx_ptr, buf);
                const typename gc_clock::duration gc_grace_sec = m.s->gc_grace_seconds();

                // The hint is too old -- drop it.
                //
                // Files are aggregated for at most hints_timer_period, so the oldest
                // hint there is (last_modification - manager::hints_timer_period) old.
                if (gc_clock::now().time_since_epoch() - secs_since_file_mod > gc_grace_sec - hints_flush_period) {
                    return seastar::make_ready_future<>();
                }

                return this->send_one_mutation(std::move(m)).then([this, ctx_ptr] {
                    ++this->shard_stats().sent;
                }).handle_exception([this, ctx_ptr] (auto eptr) {
                    manager_logger.trace("send_one_hint(): failed to send to {}: {}", _host_id, eptr);
                    return seastar::make_exception_future<>(std::move(eptr));
                });
            // Ignore these errors and move on -- probably this hint is too old and the KS/CF has been deleted...
            } catch (replica::no_such_column_family& e) {
                manager_logger.debug("send_hints(): no_such_column_family: {}", e.what());
                ++this->shard_stats().discarded;
            } catch (replica::no_such_keyspace& e) {
                manager_logger.debug("send_hints(): no_such_keyspace: {}", e.what());
                ++this->shard_stats().discarded;
            } catch (no_column_mapping& e) {
                manager_logger.debug("send_hints(): {} at {}: {}", fname, rp, e.what());
                ++this->shard_stats().discarded;
            } catch (...) {
                auto eptr = std::current_exception();
                manager_logger.debug("send_hints(): unexpected error in file {} at {}: {}", fname, rp, eptr);
                return seastar::make_exception_future<>(std::move(eptr));
            }

            return seastar::make_ready_future<>();
        }).then_wrapped([this, units = std::move(units), rp, ctx_ptr] (seastar::future<>&& f) {
            // Information about the error has already already printed somewhere before.
            // We just need to account in the ctx that sending of this hint has failed.
            if (!f.failed()) {
                ctx_ptr->on_hint_send_success(rp);
                auto new_bound = ctx_ptr->get_replayed_bound();
                // Segments from other shards are replayed first and are considered
                // to be "before" replay position 0.
                // Update the sent upper bound only if it is a local segment.
                if (new_bound.shard_id() == seastar::this_shard_id() && _sent_upper_bound_rp < new_bound) {
                    _sent_upper_bound_rp = new_bound;
                    notify_replay_waiters();
                }
            } else {
                ctx_ptr->on_hint_send_failure(rp);
            }
            f.ignore_ready_future();
        });
    }).handle_exception([ctx_ptr, rp] (auto eptr) {
        manager_logger.trace("send_one_file(): Hmm. Something bad has happened: {}", eptr);
        ctx_ptr->on_hint_send_failure(rp);
    });
}

bool manager::host_hint_manager::sender::send_one_file(const seastar::sstring& fname) {
    timespec last_mod = get_last_file_modification(fname).get0();
    typename gc_clock::duration secs_since_file_mod = std::chrono::seconds(last_mod.tv_sec);
    seastar::lw_shared_ptr<send_one_file_ctx> ctx_ptr = seastar::make_lw_shared(send_one_file_ctx{_last_schema_ver_to_column_mapping});

    try {
        commitlog::read_log_file(fname, FILENAME_PREFIX,
                [this, secs_since_file_mod, &fname, ctx_ptr] (commitlog::buffer_and_replay_position buf_rp) -> seastar::future<> {
            auto& buf = buf_rp.buffer;
            auto& rp = buf_rp.position;

            while (true) {
                // Check that we can still send the next hint. Don't try to send it if the destination host
                // is DOWN or if we have already failed to send some of the previous hints.
                if (!draining() && ctx_ptr->segment_replay_failed) {
                    co_return;
                }

                // Break early if stop() has been called or the destination node has gone down.
                if (!can_send()) {
                    ctx_ptr->segment_replay_failed = true;
                    co_return;
                }

                co_await flush_maybe();

                if (utils::get_local_injector().enter("hinted_handoff_pause_hint_replay")) {
                    // We cannot send the hint because hint replay is paused.
                    // Sleep 100ms and do the whole loop again.
                    //
                    // Jumping to the beginning of the loop ensures that
                    // - We regularly check if we should stop -- so that we don't
                    //   get stuck in shutdown.
                    // - flush_maybe() is called regularly -- so that new segments
                    //   are created and we help enforce the "at most 10s worth of
                    //   hints in a segment".
                    co_await seastar::sleep(std::chrono::milliseconds{100});
                    continue;
                } else {
                    co_await send_one_hint(ctx_ptr, std::move(buf), rp, secs_since_file_mod, fname);
                    break;
                }
            };
        }, _last_not_complete_rp.pos, &_db.extensions()).get();
    } catch (commitlog::segment_error& ex) {
        manager_logger.error("{}: {}. Dropping...", fname, ex.what());
        ctx_ptr->segment_replay_failed = false;
        ++this->shard_stats().corrupted_files;
    } catch (...) {
        manager_logger.trace("sending of {} failed: {}", fname, std::current_exception());
        ctx_ptr->segment_replay_failed = true;
    }

    // Wait till all background hints sending is complete.
    ctx_ptr->file_send_gate.close().get();

    // If we are draining, ignore failures and drop the segment even if we have failed to send it.
    if (draining() && ctx_ptr->segment_replay_failed) {
        manager_logger.trace("send_one_file(): we are draining so we are going to delete the segment anyway");
        ctx_ptr->segment_replay_failed = false;
    }

    // Update the next iteration replay position if needed.
    if (ctx_ptr->segment_replay_failed) {
        // If some hints have failed to be sent, first_failed_rp will tell the position of the first such hint.
        // If there was an error thrown by read_log_file function itself, we will retry sending from
        // the last hint that was successfully sent (last_succeeded_rp).
        _last_not_complete_rp = ctx_ptr->first_failed_rp.value_or(ctx_ptr->last_succeeded_rp.value_or(_last_not_complete_rp));
        manager_logger.trace("send_one_file(): error while sending hints from {}, last RP is {}", fname, _last_not_complete_rp);
        return false;
    }

    // If we got here, we are done with the current segment and we can remove it.
    seastar::with_shared(_file_update_mutex, [&fname, this] {
        auto p = _host_manager.get_or_load().get0();
        return p->delete_segments({ fname });
    }).get();

    // Clear the replay position -- we are going to send the next segment...
    _last_not_complete_rp = replay_position();
    _last_schema_ver_to_column_mapping.clear();
    manager_logger.trace("send_one_file(): segment {} was sent in full and deleted", fname);
    return true;
}

// Runs in the seastar::async context.
void manager::host_hint_manager::sender::send_hints_maybe() noexcept {
    manager_logger.trace("send_hints(): going to send hints to {}, we have {} segments to replay",
            _host_id, _segments_to_replay.size() + _foreign_segments_to_replay.size());
    
    int replayed_segments_count = 0;

    try {
        while (true) {
            const seastar::sstring* seg_name = name_of_current_segment();
            if (!seg_name || !replay_allowed() || !can_send()) {
                break;
            }
            if (!send_one_file(*seg_name)) {
                break;
            }
            pop_current_segment();
            ++replayed_segments_count;

            notify_replay_waiters();
        }
    // Ignore exceptions. We will retry sending this file from where we left off next time.
    // Exceptions are not expected here during the regular operation, so just log them.
    } catch (...) {
        manager_logger.trace("send_hints(): got the exception: {}", std::current_exception());
    }

    if (have_segments()) {
        // TODO: come up with something more sophisticated here
        _next_send_retry_tp = clock_type::now() + std::chrono::seconds{1};
    } else {
        // if there are no segments to send we want to retry when we maybe have some (after flushing)
        _next_send_retry_tp = _next_flush_tp;
    }

    manager_logger.trace("send_hints(): we have handled {} segments", replayed_segments_count);
}

seastar::future<> manager::host_hint_manager::sender::flush_maybe() noexcept {
    const auto current_time = clock_type::now();
    if (current_time >= _next_flush_tp) {
        try {
            co_await _host_manager.flush_current_hints();
            _next_flush_tp = current_time + hints_flush_period;
        } catch (...) {
            manager_logger.trace("flush_maybe() has failed: {}", std::current_exception());
        }
    }
}

void manager::host_hint_manager::sender::notify_replay_waiters() noexcept {
    if (!_foreign_segments_to_replay.empty()) {
        manager_logger.trace("[{}] notify_replay_waiters(): not notifying because there are still {} foreign segments to replay",
                _host_id, _foreign_segments_to_replay.size());
        return;
    }

    manager_logger.trace("[{}] notify_replay_waiters(): replay position upper bound was updated to {}",
            _host_id, _sent_upper_bound_rp);
    const auto rw_it = _replay_waiters.begin();
    while (!_replay_waiters.empty() && rw_it->first < _sent_upper_bound_rp) {
        manager_logger.trace("[{}] notify_replay_waiters(): notifying one ({} < {})",
                _host_id, rw_it->first, _sent_upper_bound_rp);
        auto ptr = rw_it->second;
        (**ptr).set_value();
        (*ptr) = std::nullopt; // Prevent it from being resolved by abort source subscription.
        _replay_waiters.erase(rw_it);
    }
}

void manager::host_hint_manager::sender::dismiss_replay_waiters() noexcept {
    for (auto& p : _replay_waiters) {
        manager_logger.debug("[{}] dismiss_replay_waiters(): dismissing one", _host_id);
        auto ptr = p.second;
        (**ptr).set_exception(std::runtime_error{seastar::format("Hints manager for {} is stopping", _host_id)});
        (*ptr) = std::nullopt; // Prevent it from being resolved by abort source subscription.
    }
    _replay_waiters.clear();
}

typename manager::host_hint_manager::sender::time_duration_type manager::host_hint_manager::sender::next_sleep_duration() const {
    const time_point_type current_time = clock_type::now();
    const time_point_type next_flush_tp = std::max(_next_flush_tp, current_time);
    const time_point_type next_retry_tp = std::max(_next_send_retry_tp, current_time);

    const time_duration_type d = std::min(next_flush_tp, next_retry_tp) - current_time;

    // Don't sleep for less than 10 ticks of the "clock" if we are planning to sleep at all
    // -- the sleep() function is not perfect.
    return time_duration_type{10 * div_ceil(d.count(), 10)};
}



/////////////////////////
/////////////////////////
//.....................//
//..host hint manager..//
//.....................//
/////////////////////////
/////////////////////////



manager::host_hint_manager::host_hint_manager(const locator::host_id& host_id, manager& shard_manager)
    : _host_id{host_id}
    , _file_update_mutex_ptr{seastar::make_lw_shared(seastar::shared_mutex{})}
    , _shard_manager{shard_manager}
    , _sender{*this, _shard_manager.local_storage_proxy(),
            _shard_manager.local_db(), _shard_manager.local_gossiper()}
    , _state{hint_manager_state_set::of<hint_manager_state::stopped>()}
    , _hints_dir{_shard_manager.hints_dir() / seastar::format("{}", _host_id).c_str()}
    // Approximate the position of the last written hint by using the same formula
    // as for segment id calculation in commitlog.
    // TODO: Should this logic be deduplicated with what is in the commitlog?
    , _last_written_rp{seastar::this_shard_id(),
            std::chrono::duration_cast<std::chrono::milliseconds>(runtime::get_boot_time().time_since_epoch()).count()}
{}

manager::host_hint_manager::host_hint_manager(host_hint_manager&& other)
    : _host_id{other._host_id}
    , _file_update_mutex_ptr{other._file_update_mutex_ptr}
    , _shard_manager{other._shard_manager}
    , _sender{other._sender, *this}
    , _state{other._state}
    , _hints_dir{std::move(other._hints_dir)}
    , _last_written_rp{other._last_written_rp}
{}

manager::host_hint_manager::~host_hint_manager() {
    assert(stopped());
}

void manager::host_hint_manager::start() {
    clear_stopped();
    allow_hints();
    _sender.start();
}

seastar::future<> manager::host_hint_manager::stop(drain should_drain) noexcept {
    if (stopped()) {
        return seastar::make_exception_future(
                std::logic_error{seastar::format("host_manager[{}]: stop() has been called twice", _host_id).c_str()});
    }

    return seastar::async([this, should_drain] {
        std::exception_ptr eptr;

        // This is going to prevent further storing of new hints and will break all sending in progress.
        set_stopping();

        try {
            _store_gate.close().get();
            _sender.stop(should_drain).get();
        } catch (...) {
            eptr = std::current_exception();
        }

        seastar::with_lock(file_update_mutex(), [this] {
            if (_hints_store_anchor) {
                hints_store_ptr tmp = std::exchange(_hints_store_anchor, nullptr);
                return tmp->shutdown().finally([tmp] {
                    return tmp->release();
                }).finally([tmp] {});
            }
            return seastar::make_ready_future<>();
        }).get();

        if (eptr) {
            manager_logger.error("host_manager[{}]: exception: {}", _host_id, eptr);
        }

        set_stopped();
    });
}

seastar::future<hints_store_ptr> manager::host_hint_manager::get_or_load() {
    namespace sc = seastar::coroutine;

    if (!_hints_store_anchor) {
        const auto log_ptr = co_await _shard_manager.store_factory().get_or_load(_host_id,
                sc::lambda([this] (const locator::host_id&) noexcept {
            return add_store();
        }));
        _hints_store_anchor = log_ptr;
        co_return log_ptr;
    }

    co_return _hints_store_anchor;
}

bool manager::host_hint_manager::store_hint(schema_ptr s, seastar::lw_shared_ptr<const frozen_mutation> fm,
        tracing::trace_state_ptr tr_state) noexcept
{
    try {
        // Future is waited on indirectly in `stop()` (via `_store_gate`).
        (void) seastar::with_gate(_store_gate, [this, s, fm, tr_state] () mutable {
            ++_hints_in_progress;
            size_t mut_size = fm->representation().size();
            shard_stats().size_of_hints_in_progress += mut_size;

            return seastar::with_shared(file_update_mutex(), [this, fm, s, tr_state] () mutable -> seastar::future<> {
                return get_or_load().then([fm, s, tr_state] (hints_store_ptr log_ptr) mutable {
                    commitlog_entry_writer cew{s, *fm, commitlog::force_sync::no};
                    return log_ptr->add_entry(s->id(), cew, timeout_clock::now() + HINT_FILE_WRITE_TIMEOUT);
                }).then([this, tr_state] (rp_handle rh) {
                    auto rp = rh.release();
                    if (_last_written_rp < rp) {
                        _last_written_rp = rp;
                        manager_logger.debug("[{}] Updated last written replay position to {}", _host_id, rp);
                    }
                    ++shard_stats().written;

                    manager_logger.trace("Hint to {} was stored", _host_id);
                    tracing::trace(tr_state, "Hint to {} was stored", _host_id);
                }).handle_exception([this, tr_state] (std::exception_ptr eptr) {
                    ++shard_stats().errors;

                    manager_logger.debug("store_hint(): got the exception when storing a hint to {}: {}", _host_id, eptr);
                    tracing::trace(tr_state, "Failed to store a hint to {}: {}", _host_id, eptr);
                });
            }).finally([this, mut_size, fm, s] {
                --_hints_in_progress;
                shard_stats().size_of_hints_in_progress -= mut_size;
            });
        });
    } catch (...) {
        const auto eptr = std::current_exception();
        manager_logger.trace("Failed to store a hint to {}: {}", _host_id, eptr);
        tracing::trace(tr_state, "Failed to store a hint to {}: {}", _host_id, eptr);

        ++shard_stats().dropped;
        return false;
    }

    // ??? This doesn't seem to make sense to me because we don't wait for the with_gate to finish,
    // so we probably always return true here... Or maybe not?
    return true;
}

seastar::future<> manager::host_hint_manager::populate_segments_to_replay() {
    return seastar::with_lock(file_update_mutex(), [this] {
        return get_or_load().discard_result();
    });
}

seastar::future<commitlog> manager::host_hint_manager::add_store() noexcept {
    manager_logger.trace("Going to add a store to {}", _hints_dir.c_str());

    namespace sc = seastar::coroutine;
    co_return co_await seastar::futurize_invoke(sc::lambda([this] () -> seastar::future<commitlog> {
        co_await io_check(sc::lambda([name = _hints_dir.c_str()] {
            return recursive_touch_directory(name);
        }));
        commitlog::config cfg;
            
        cfg.commit_log_location = _hints_dir.c_str();
        cfg.commitlog_segment_size_in_mb = resource_manager::hint_segment_size_in_mb;
        cfg.commitlog_total_space_in_mb = resource_manager::max_hints_per_host_size_mb;
        cfg.fname_prefix = FILENAME_PREFIX;
        cfg.extensions = &_shard_manager.local_db().extensions();

        // HH leaves segments on disk after commitlog shutdown, and later reads
        // them when commitlog is re-created. This is expected to happen regularly
        // during standard HH workload, so no need to print a warning about it.
        cfg.warn_about_segments_left_on_disk_after_shutdown = false;
        // Allow going over the configured size limit of the commitlog
        // (resource_manager::max_hints_per_ep_size_mb). The commitlog will
        // be more conservative with its disk usage when going over the limit.
        // On the other hand, HH counts used space using the space_watchdog
        // in resource_manager, so its redundant for the commitlog to apply
        // a hard limit.
        cfg.allow_going_over_size_limit = true;
        // The API for waiting for hint replay relies on replay positions
        // monotonically increasing. When there are no segments on disk,
        // by default the commitlog will calculate the first segment ID
        // based on the boot time. This may cause the following sequence
        // of events to occur:
        //
        // 1. Node starts with empty hints queue
        // 2. Some hints are written and some segments are created
        // 3. All hints are replayed
        // 4. Hint sync point is created
        // 5. Commitlog instance gets re-created and resets it segment ID counter
        // 6. New hint segment has the first ID as the first (deleted by now) segment
        // 7. Waiting for the sync point commences but resolves immediately
        //    before new hints are replayed - since point 5., `_last_written_rp`
        //    and `_sent_upper_bound_rp` are not updated because RPs of new
        //    hints are much lower than both of those marks.
        //
        // In order to prevent this situation, we override the base segment ID
        // of the newly created commitlog instance - it should start with an ID
        // which is larger than the segment ID of the RP of the last written hint.
        cfg.base_segment_id = _last_written_rp.base_id();

        commitlog l = co_await commitlog::create_commitlog(std::move(cfg));
        // add_store() is triggered every time hint files are forcefully flushed to I/O (every hints_flush_period).
        // When this happens we want to refill _sender's segments only if it has finished with the segments he had before.
        if (_sender.have_segments()) {
            co_return l;
        }

        std::vector<seastar::sstring> segs_vec = co_await l.get_segments_to_replay();

        if (segs_vec.empty()) {
            // If the segs_vec is empty, this means that there are no more
            // hints to be replayed. We can safely skip to the position of the
            // last written hint.
            //
            // This is necessary: remember that we artificially set
            // the last replayed position based on the creation time
            // of the endpoint manager. If we replay all segments from
            // previous runtimes but won't write any new hints during
            // this runtime, then without the logic below the hint replay
            // tracker won't reach the hint written tracker.
            auto rp = _last_written_rp;
            rp.pos++;
            _sender.rewind_sent_replay_position_to(rp);
            co_return l;
        }

        std::vector<std::pair<segment_id_type, seastar::sstring>> local_segs_vec;
        local_segs_vec.reserve(segs_vec.size());

        // Divide segments into those that were created on this shard
        // and those which were moved to it during rebalancing.
        for (auto& seg : segs_vec) {
            commitlog::descriptor desc(seg, FILENAME_PREFIX);
            unsigned shard_id = replay_position(desc).shard_id();
            if (shard_id == this_shard_id()) {
                local_segs_vec.emplace_back(desc.id, std::move(seg));
            } else {
                _sender.add_foreign_segment(std::move(seg));
            }
        }

        // Sort local segments by their segment ids, which should
        // correspond to the chronological order.
        std::sort(local_segs_vec.begin(), local_segs_vec.end());

        for (auto& [_, seg] : local_segs_vec) {
            _sender.add_segment(std::move(seg));
        }

        co_return l;
    }));
}

seastar::future<> manager::host_hint_manager::flush_current_hints() noexcept {
    // Flush created hints to disk.
    if (_hints_store_anchor) {
        return seastar::futurize_invoke([this] {
            return seastar::with_lock(file_update_mutex(), [this] {
                return get_or_load().then([] (hints_store_ptr cptr) {
                    return cptr->shutdown().finally([cptr] {
                        return cptr->release();
                    }).finally([cptr] {});
                }).then([this] {
                    // Un-hold the commitlog object. Since we are under the exclusive _file_update_mutex
                    // lock there are no other hints_store_ptr copies and this would destroy
                    // the commitlog shared value.
                    _hints_store_anchor = nullptr;

                    // Re-create the commitlog instance - this will re-populate the _segments_to_replay
                    // if needed.
                    return get_or_load().discard_result();
                });
            });
        });
    }

    return seastar::make_ready_future<>();
}



/////////////////////
/////////////////////
//.................//
//.....manager.....//
//.................//
/////////////////////
/////////////////////



namespace {

using shard_id_type = unsigned;
using host_hints_segment_map = std::unordered_map<shard_id_type, std::list<fs::path>>;
using hints_segment_map = std::unordered_map<locator::host_id, host_hints_segment_map>;

// TODO: Couldn't this be templated so that we can avoid using std::function and, consequently, allocating memory?
//       Look where exactly this function is called.
seastar::future<> scan_for_hints_dirs(const seastar::sstring& hints_directory,
        std::function<seastar::future<> (fs::path dir, seastar::directory_entry de, shard_id_type shard_id)> f)
{
    return lister::scan_dir(hints_directory, lister::dir_entry_types::of<seastar::directory_entry_type::directory>(),
            [f = std::move(f)] (fs::path dir, seastar::directory_entry de) mutable {
        shard_id_type shard_id;
        try {
            shard_id = std::stoi(de.name.c_str());
        } catch (std::invalid_argument& ex) {
            manager_logger.debug("Ignore invalid directory {}", de.name);
            return seastar::make_ready_future<>();
        }
        return f(std::move(dir), std::move(de), shard_id);
    });
}

/// \brief Scan the given hints directory and build a map of all present hints segments.
///
/// Complexity: O(N+K), where N is a total number of present hints' segments and
///                           K = <number of shards during the previous boot> * <number of end points for which hints where ever created>
///
/// \note Should be called from a seastar::thread context.
///
/// \param hints_directory directory to scan
/// \return a map: host_id -> (map: shard -> segments (full paths))
hints_segment_map get_current_hints_segments(const seastar::sstring& hints_directory) {
    hints_segment_map current_hints_segments;

    // Shard level.
    scan_for_hints_dirs(hints_directory,
            [&current_hints_segments] (fs::path dir, seastar::directory_entry de, shard_id_type shard_id) {
        manager_logger.trace("shard_id = {}", shard_id);

        // IP level.
        return lister::scan_dir(dir / de.name.c_str(), lister::dir_entry_types::of<seastar::directory_entry_type::directory>(),
                [&current_hints_segments, shard_id] (fs::path dir, seastar::directory_entry de) {
            manager_logger.trace("\tHost ID: {}", de.name);
            const auto host_id = locator::host_id{utils::UUID{de.name}};

            // Hint files.
            return lister::scan_dir(dir / de.name.c_str(), lister::dir_entry_types::of<seastar::directory_entry_type::regular>(),
                    [&current_hints_segments, shard_id, host_id] (fs::path dir, seastar::directory_entry de) {
                manager_logger.trace("\t\tFile: {}", de.name);
                current_hints_segments[host_id][shard_id].emplace_back(dir / de.name.c_str());
                return seastar::make_ready_future<>();
            });
        });
    }).get();

    return current_hints_segments;
}

/// \brief Rebalance hints segments for a given (destination) end point
///
/// This method is going to consume files from the \ref segments_to_move and distribute them between the present
/// shards (taking into an account the \ref host_segments state - there may be zero or more segments that belong to a
/// particular shard in it) until we either achieve the requested \ref segments_per_shard level on each shard
/// or until we are out of files to move.
///
/// As a result (in addition to the actual state on the disk) both \ref host_segments and \ref segments_to_move are going
/// to be modified.
///
/// Complexity: O(N), where N is a total number of present hints' segments for the \ref host_id host (as a destination).
///
/// \note Should be called from a seastar::thread context.
///
/// \param host_id destination host_id
/// \param segments_per_shard number of hints segments per-shard we want to achieve
/// \param hints_directory a root hints directory
/// \param host_segments a map that was originally built by get_current_hints_segments() for this host
/// \param segments_to_move a list of segments we are allowed to move
void rebalance_segments_for(const locator::host_id& host_id, size_t segments_per_shard,
        const seastar::sstring& hints_directory, host_hints_segment_map& host_segments,
        std::list<fs::path>& segments_to_move)
{
    manager_logger.trace("{}: segments_per_shard: {}, total number of segments to move: {}",
            host_id, segments_per_shard, segments_to_move.size());
    
    if (segments_to_move.empty() || !segments_per_shard) {
        return;
    }

    for (shard_id_type i = 0; i < smp::count && !segments_to_move.empty(); ++i) {
        fs::path shard_path_dir{fs::path{hints_directory.c_str()} / seastar::format("{:d}", i).c_str() / host_id.to_sstring()};
        std::list<fs::path>& current_shard_segments = host_segments[i];

        // Make sure that the shard_path_dir exists. If not -- create it.
        io_check([name = shard_path_dir.c_str()] {
            return seastar::recursive_touch_directory(name);
        }).get();

        while (current_shard_segments.size() < segments_per_shard && !segments_to_move.empty()) {
            auto seg_path_it = segments_to_move.begin();
            fs::path new_path{shard_path_dir / seg_path_it->filename()};

            // Don't move the file to the same location -- it's pointless.
            if (*seg_path_it != new_path) {
                manager_logger.trace("going to move: {} -> {}", *seg_path_it, new_path);
                io_check(rename_file, seg_path_it->native(), new_path.native()).get();
            } else {
                manager_logger.trace("skipping: {}", *seg_path_it);
            }
            
            current_shard_segments.splice(current_shard_segments.end(), segments_to_move, seg_path_it, std::next(seg_path_it));
        }
    }
}
    
/// \brief Rebalance all present hints segments.
///
/// The difference between the number of segments on every two shard will be not greater than 1 after the
/// rebalancing.
///
/// Complexity: O(N), where N is a total number of present hints' segments.
///
/// \note Should be called from a seastar::thread context.
///
/// \param hints_directory a root hints directory
/// \param segments_map a map that was built by get_current_hints_segments()
void rebalance_segments(const seastar::sstring& hints_directory, hints_segment_map& segments_map) {
    // Count how many hint segments to each destination we have.
    std::unordered_map<locator::host_id, size_t> hints_per_host;

    for (const auto& [host_id, host_hints_map] : segments_map) {
        hints_per_host[host_id] = std::transform_reduce(
                host_hints_map.begin(), host_hints_map.end(),
                size_t{0}, std::plus<>(),
                [] (const auto& pair) { return pair.second.size(); });

        manager_logger.trace("{}: total files: {}", host_id, hints_per_host[host_id]);
    }

    // Create a map of lists of segments that we will move (for each destination host):
    // if a shard has segments, then we will NOT move q = int(N/S) segments out of them,
    // where N is the total number of segments to the current destination
    // and S is the current number of shards.
    std::unordered_map<locator::host_id, std::list<fs::path>> segments_to_move;
    for (auto& [host_id, host_segments] : segments_map) {
        const size_t q = hints_per_host[host_id] / smp::count;
        auto& current_segments_to_move = segments_to_move[host_id];

        for (auto& [shard_id, shard_segments] : host_segments) {
            // Move all segments from the shards that are no longer relevant (re-sharding to the lower number of shards)
            if (shard_id >= smp::count) {
                current_segments_to_move.splice(current_segments_to_move.end(), shard_segments);
            } else if (shard_segments.size() > q) {
                current_segments_to_move.splice(
                        current_segments_to_move.end(),
                        shard_segments,
                        std::next(shard_segments.begin(), q),
                        shard_segments.end());
            }
        }
    }

    // Since N (the total number of segments to a specific destination) may not be a multiple of S (the current number of
    // shards), we will distribute files in two passes:
    //    * if N = S * q + r, then
    //       * one pass for segments_per_shard = q
    //       * another one for segments_per_shard = q + 1.
    //
    // This way we will get as close to the perfect distribution as possible.
    //
    // Right till this point, we haven't moved any segments. However, we have created a logical separation of segments
    // into two groups:
    //    * Segments that are not going to be moved: segments in the segments_map.
    //    * Segments that are going to be moved: segments in the segments_to_move.
    //
    // rebalance_segments_for() is going to consume segments from segments_to_move and move them to corresponding
    // lists in the segments_map AND actually move segments to the corresponding shard's sub-directory until the requested
    // segments_per_shard level is reached (see more details in the description of rebalance_segments_for()).
    for (const auto& [host_id, N] : hints_per_host) {
        const auto q = N / smp::count;
        const auto r = N - smp::count * q;
        auto& current_segments_to_move = segments_to_move[host_id];
        auto& current_segments_map = segments_map[host_id];

        if (q) {
            rebalance_segments_for(host_id, q, hints_directory, current_segments_map, current_segments_to_move);
        }
        if (r) {
            rebalance_segments_for(host_id, q + 1, hints_directory, current_segments_map, current_segments_to_move);
        }
    }
}

/// \brief Remove sub-directories of shards that are not relevant any more (re-sharding to a lower number of shards case).
///
/// Runs in seastar::async context.
///
/// Complexity: O(S*E), where S is the number of shards during the previous boot and
///                           E is the number of hosts for which hints where ever created.
///
/// \param hints_directory a root hints directory
void remove_irrelevant_shards_directories(const seastar::sstring& hints_directory) {
    // Shard level.
    scan_for_hints_dirs(hints_directory, [] (fs::path dir, directory_entry de, shard_id_type shard_id) {
        if (shard_id >= smp::count) {
            // IP level
            return lister::scan_dir(dir / de.name.c_str(), lister::dir_entry_types::full(), lister::show_hidden::yes,
                    [] (fs::path dir, seastar::directory_entry de) {
                    return io_check(remove_file, (dir / de.name.c_str()).native());
            }).then([shard_base_dir = dir, shard_entry = de] {
                return io_check(remove_file, (shard_base_dir / shard_entry.name.c_str()).native());
            });
        }
        return seastar::make_ready_future<>();
    }).get();
}

} // anonymous namespace



manager::manager(seastar::sstring hints_directory, host_filter filter, int64_t max_hint_window_ms,
        resource_manager& res_manager, seastar::sharded<replica::database>& db)
    : _hints_dir{fs::path{hints_directory} / seastar::format("{:d}", this_shard_id())}
    , _host_filter{std::move(filter)}
    , _max_hint_window_us{max_hint_window_ms * 1'000}
    , _local_db{db.local()}
    , _resource_manager{res_manager}
{
    if (utils::get_local_injector().enter("decrease_hints_flush_period")) {
        hints_flush_period = std::chrono::seconds(1);
    }
}

manager::~manager() noexcept {
    assert(_host_managers.empty());
}

seastar::future<> manager::start(seastar::shared_ptr<service::storage_proxy> proxy_ptr, seastar::shared_ptr<gms::gossiper> gossiper_ptr) {
    _proxy_anchor = std::move(proxy_ptr);
    _gossiper_anchor = std::move(gossiper_ptr);

    namespace sc = seastar::coroutine;
    co_await lister::scan_dir(_hints_dir, lister::dir_entry_types::of<directory_entry_type::directory>(),
            sc::lambda([this] (fs::path datadir, seastar::directory_entry de) -> seastar::future<> {
        const auto host_id = locator::host_id{utils::UUID{de.name}};
        if (!check_dc_for(host_id)) {
            co_return;
        }
        co_await get_host_manager(host_id).populate_segments_to_replay();
    }));
    co_await compute_hints_dir_device_id();
    set_started();
}

seastar::future<> manager::stop() {
    manager_logger.info("Asked to stop");
    set_stopping();

    return _draining_hosts_gate.close().finally([this] {
        return seastar::parallel_for_each(_host_managers, [] (auto& pair) {
            return pair.second.stop();
        }).finally([this] {
            _host_managers.clear();
            manager_logger.info("Stopped");
        }).discard_result();
    });
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

bool manager::store_hint(const locator::host_id& host_id, schema_ptr s,
        seastar::lw_shared_ptr<const frozen_mutation> fm, tracing::trace_state_ptr tr_state) noexcept
{
    if (stopping() || draining_all() || !started() || !can_hint_for(host_id)) {
        manager_logger.trace("Can't store a hint to {}", host_id);
        ++_stats.dropped;
        return false;
    }

    try {
        manager_logger.trace("Going to store a hint to {}", host_id);
        tracing::trace(tr_state, "Going to store a hint to {}", host_id);

        return get_host_manager(host_id).store_hint(std::move(s), std::move(fm), tr_state);
    } catch (...) {
        const auto eptr = std::current_exception();
        manager_logger.trace("Failed to store a hint to {}: {}", host_id, eptr);
        tracing::trace(tr_state, "Failed to store a hint to {}: {}", host_id, eptr);

        ++_stats.errors;
        return false;
    }
}

seastar::future<> manager::change_host_filter(host_filter filter) {
    if (!started()) {
        throw std::logic_error{"change_host_filter: called before the hints_manager was started"};
    }

    seastar::gate::holder holder{_draining_hosts_gate};
    
    namespace sc = seastar::coroutine;
    co_await seastar::with_semaphore(drain_lock(), 1,
            sc::lambda([this, filter = std::move(filter)] () mutable -> seastar::future<> {
        if (draining_all()) {
            throw std::logic_error{"change_host_filter: cannot change the configuration because all hints have been drained"};
        }

        manager_logger.debug("change_host_filter: changing from {} to {}", _host_filter, filter);

        // Change the host_filter now, but save the old one to roll back if a failure occurs.
        std::swap(_host_filter, filter);

        const auto& topology = _proxy_anchor->get_token_metadata_ptr()->get_topology();
        std::exception_ptr eptr = nullptr;
            
        try {
            // Iterate over existing hint directories and see if we can enable a host manager for some of them.
            co_await lister::scan_dir(_hints_dir, lister::dir_entry_types::of<seastar::directory_entry_type::directory>(),
                    sc::lambda([this, &topology] (fs::path datadir, seastar::directory_entry de) -> seastar::future<> {
                const auto host_id = locator::host_id{utils::UUID{de.name}};

                if (_host_managers.contains(host_id) || !_host_filter.can_hint_for(topology, host_id)) {
                    co_return;
                }

                co_await get_host_manager(host_id).populate_segments_to_replay();
            }));
        } catch (...) {
            // Bring back the old filter. The finally() block will cause us to stop
            // the additional ep_hint_managers that we started
            _host_filter = std::move(filter);
            eptr = std::current_exception();
        }

        // Remove host managers which are rejected by the filter.
        co_await sc::parallel_for_each(_host_managers, sc::lambda([this, &topology] (auto& pair) -> seastar::future<> {
            auto& [id, hmanager] = pair;
                
            if (_host_filter.can_hint_for(topology, id)) {
                co_return;
            }

            co_await hmanager.stop(drain::no).finally([this, id = id] {
                _host_managers.erase(id);
            });
        }));

        if (eptr) {
            std::rethrow_exception(eptr);
        }
    }));
}

bool manager::can_hint_for(const locator::host_id& host_id) const noexcept {
    const auto tm_ptr = get_token_metadata_ptr(_proxy_anchor);
    if (!tm_ptr) [[unlikely]] {
        return false;
    }

    const auto my_host_id = tm_ptr->get_my_id();
    if (host_id == my_host_id) {
        return false;
    }

    auto it = _host_managers.find(host_id);
    if (it != _host_managers.end() && (it->second.stopping() || !it->second.can_hint())) {
        return false;
    }

    const auto hipf = hints_in_progress_for(host_id);
    // Don't allow more than one in-flight (to the store) hint to a specific destination
    // when the total size of in-flight hints is more than the maximum allowed value.
    //
    // In the worst case, there's going to be (_max_size_of_hints_in_progress + N - 1) in-flight hints,
    // where N is the total number nodes in the cluster.
    if (_stats.size_of_hints_in_progress > max_size_of_hints_in_progress && hipf > 0) {
        manager_logger.trace("size_of_hints_in_progress {} hints_in_progress_for({}) {}",
                _stats.size_of_hints_in_progress, host_id, hipf);
        return false;
    }

    // Check that the destination DC is "hintable".
    if (!check_dc_for(host_id)) {
        manager_logger.trace("{}'s DC is not hintable", host_id);
        return false;
    }

    const std::optional<gms::inet_address> maybe_ep = tm_ptr->get_endpoint_for_host_id(host_id);
    if (!maybe_ep.has_value()) {
        manager_logger.debug("Host of ID {} doesn't exist", host_id);
        return false;
    }

    const auto ep_downtime = local_gossiper().get_endpoint_downtime(maybe_ep.value());
    // Check if the host has been down for too long.
    if (ep_downtime > _max_hint_window_us) {
        manager_logger.trace("{} has been down for {}, not hinting", host_id, ep_downtime);
        return false;
    }

    return true;
}

bool manager::too_many_in_flight_hints_for(const locator::host_id& host_id) const noexcept {
    const locator::token_metadata_ptr tm_ptr = get_token_metadata_ptr(_proxy_anchor);
    if (!tm_ptr) [[unlikely]] {
        return false;
    }

    const locator::host_id my_host_id = tm_ptr->get_my_id();
    const std::optional<gms::inet_address> maybe_ep = tm_ptr->get_endpoint_for_host_id(host_id);
    
    if (!maybe_ep.has_value()) {
        return false;
    }

    // There is no need to check the DC here because if there is an in-flight hint
    // for this host, then it means that its DC has already been checked
    // and found to be ok.
    return _stats.size_of_hints_in_progress > max_size_of_hints_in_progress &&
            host_id != my_host_id &&
            hints_in_progress_for(host_id) > 0 &&
            local_gossiper().get_endpoint_downtime(maybe_ep.value()) <= _max_hint_window_us;
}

bool manager::check_dc_for(const locator::host_id& host_id) const noexcept {
    const auto& topology = _proxy_anchor->get_token_metadata_ptr()->get_topology();
    try {
        // If the target's DC is not a "hintable" DCs -- don't hint.
        // If there is a host manager, then DC has already been checked and found to be ok.
        return _host_filter.is_enabled_for_all() ||
                have_host_manager(host_id) ||
                _host_filter.can_hint_for(topology, host_id);
    } catch (...) {
        // Failing to check the DC should block the hint.
        return false;
    }
}

void manager::allow_hints() {
    for (auto& [_, host_manager] : _host_managers) {
        host_manager.allow_hints();
    }
}

void manager::forbid_hints() {
    for (auto& [_, host_manager] : _host_managers) {
        host_manager.forbid_hints();
    }
}

void manager::forbid_hints_for_hosts_with_pending_hints() {
    manager_logger.trace("space_watchdog: Going to block hints to: {}", _hosts_with_pending_hints);
    for (auto& [id, host_manager] : _host_managers) {
        if (has_host_with_pending_hints(id)) {
            host_manager.forbid_hints();
        } else {
            host_manager.allow_hints();
        }
    }
}

sync_point::shard_rps manager::calculate_current_sync_point(const std::vector<locator::host_id>& target_hosts) const {
    sync_point::shard_rps rps;
    for (const auto& host_id : target_hosts) {
        auto it = _host_managers.find(host_id);
        if (it != _host_managers.end()) {
            const host_hint_manager& man = it->second;
            rps[host_id] = man.last_written_replay_position();
        }
    }
    return rps;
}

seastar::future<> manager::wait_for_sync_point(seastar::abort_source& as, const sync_point::shard_rps& rps) {
    seastar::abort_source local_as;

    auto sub = as.subscribe([&local_as] () noexcept {
        if (!local_as.abort_requested()) {
            local_as.request_abort();
        }
    });

    if (as.abort_requested()) {
        local_as.request_abort();
    }

    bool was_aborted = false;
    return seastar::parallel_for_each(_host_managers, [&was_aborted, &rps, &local_as] (auto& p) {
        auto& [host_id, host_manager] = p;
        replay_position rp;

        auto it = rps.find(host_id);
        if (it != rps.end()) {
            rp = it->second;
        }

        return host_manager.wait_until_hints_are_replayed_up_to(local_as, rp).handle_exception([&local_as, &was_aborted] (auto eptr) {
            if (!local_as.abort_requested()) {
                local_as.request_abort();
            }

            try {
                std::rethrow_exception(std::move(eptr));
            } catch (const seastar::abort_requested_exception&) {
                was_aborted = true;
            } catch (...) {
                return seastar::make_exception_future(std::current_exception());
            }

            return seastar::make_ready_future<>();
        });
    });

    if (was_aborted) {
        return seastar::make_exception_future(seastar::abort_requested_exception{});
    }
}

// TODO: Decide if the function should be removed from the API or if it should be implemented.
// directory_initializer manager::make_directory_initializer(utils::directories& dirs, fs::path hints_directory);

seastar::future<> manager::rebalance(seastar::sstring hints_directory) {
    return seastar::async([hints_directory = std::move(hints_directory)] {
        // Scan the currently present hints segments.
        hints_segment_map current_hints_segments = get_current_hints_segments(hints_directory);

        // Move segments to achieve an even distribution of files among all present shards.
        rebalance_segments(hints_directory, current_hints_segments);

        // Remove the directories of shards that are not present anymore
        // -- they should not have any segments by now.
        remove_irrelevant_shards_directories(hints_directory);
    });
}

void manager::drain_for(const locator::host_id& host_id) {
    if (!started() || stopping() || draining_all()) {
        return;
    }

    manager_logger.trace("on_leave_cluster: {} is removed/decommissioned", host_id);

    // The future is waited on indirectly in `stop()` (via `_draining_hosts_gate`).
    (void) seastar::with_gate(_draining_hosts_gate, [this, host_id = host_id] {
        return seastar::with_semaphore(drain_lock(), 1, [this, host_id] {
            return seastar::futurize_invoke([this, host_id] {
                const auto my_host_id = _proxy_anchor->get_token_metadata_ptr()->get_my_id();

                if (host_id == my_host_id) {
                    set_draining_all();
                    return parallel_for_each(_host_managers, [] (auto& pair) {
                        auto& [_, hmanager] = pair;
                        return hmanager.stop(drain::yes).finally([&hmanager = hmanager] {
                            return with_file_update_mutex(hmanager, [&hmanager] {
                                return seastar::remove_file(hmanager.hints_dir().c_str());
                            });
                        });
                    }).finally([this] {
                        _host_managers.clear();
                    });
                } else { // if host_id != my_host_id
                    auto hmanager_it = _host_managers.find(host_id);
                    if (hmanager_it != _host_managers.end()) {
                        auto& [_, hmanager] = *hmanager_it;
                        return hmanager.stop(drain::yes).finally([this, host_id, &hmanager = hmanager] {
                            return with_file_update_mutex(hmanager, [&hmanager] {
                                return seastar::remove_file(hmanager.hints_dir().c_str());
                            }).finally([this, host_id] {
                                _host_managers.erase(host_id);
                            });
                        });
                    }

                    return seastar::make_ready_future<>();
                }
            }).handle_exception([host_id] (auto eptr) {
                manager_logger.error("Exception when draining {}: {}", host_id, eptr);
            });
        });
    }).finally([host_id = host_id] {
        manager_logger.trace("drain_for: finished draining {}", host_id);
    });
}

void manager::update_backlog(size_t backlog, size_t max_backlog) {
    if (backlog < max_backlog) {
        allow_hints();
    } else {
        forbid_hints_for_hosts_with_pending_hints();
    }
}

seastar::future<> manager::compute_hints_dir_device_id() {
    try {
        _hints_dir_device_id = co_await get_device_id(_hints_dir.native());
    } catch (...) {
        manager_logger.warn("Failed to stat directory {} for device id: {}", _hints_dir.native(), std::current_exception());
        throw;
    }
}

manager::host_hint_manager& manager::get_host_manager(const locator::host_id& host_id) {
    auto it = _host_managers.find(host_id);
    if (it == _host_managers.end()) {
        manager_logger.trace("Creating a hint_host_manager for {}", host_id);
        manager::host_hint_manager& hhman = _host_managers.emplace(host_id, host_hint_manager{host_id, *this}).first->second;
        hhman.start();
        return hhman;
    }
    return it->second;
}

} // namespace db::hints
