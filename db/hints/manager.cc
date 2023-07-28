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
#include <numeric>
#include <optional>
#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/file-types.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/print.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/shared_mutex.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/smp.hh>
#include <seastar/coroutine/parallel_for_each.hh>

// Scylla includes.
#include "converting_mutation_partition_applier.hh"
#include "db/commitlog/commitlog.hh"
#include "db/commitlog/commitlog_entry.hh"
#include "db/commitlog/replay_position.hh"
#include "db/hints/resource_manager.hh"
#include "dht/i_partitioner.hh"
#include "gms/inet_address.hh"
#include "inet_address_vectors.hh"
#include "locator/topology.hh"
#include "log.hh"
#include "mutation/frozen_mutation.hh"
#include "mutation/mutation.hh"
#include "schema/schema.hh"
#include "schema/schema_fwd.hh"
#include "seastar/core/semaphore.hh"
#include "seastar/core/sleep.hh"
#include "seastar/core/thread.hh"
#include "utils/disk-error-handler.hh"
#include "utils/div_ceil.hh"
#include "utils/error_injection.hh"
#include "utils/fb_utilities.hh"
#include "utils/lister.hh"
#include "utils/runtime.hh"

// STD.
#include <chrono>
#include <stdexcept>
#include <utility>

namespace db::hints {

static logging::logger manager_logger("hints_manager");

const std::string manager::FILENAME_PREFIX("HintsLog" + commitlog::descriptor::SEPARATOR);
const std::chrono::seconds manager::hint_file_write_timeout = std::chrono::seconds(2);
std::chrono::seconds manager::hints_flush_period = std::chrono::seconds(10);

manager::manager(seastar::sstring hints_directory, host_filter filter, int64_t max_hint_window_ms,
        resource_manager& res_manager, sharded<replica::database>& db)
    : _hints_dir(fs::path(hints_directory) / seastar::format("{:d}", this_shard_id()))
    , _host_filter(std::move(filter))
    , _max_hint_window_us(max_hint_window_ms * 1'000)
    , _local_db(db.local())
    , _resource_manager(res_manager)
{
    if (utils::get_local_injector().enter("decrease_hints_flush_period")) {
        hints_flush_period = std::chrono::seconds(1);
    }
}

manager::~manager() {
    assert(_host_managers.empty());
}

void manager::register_metrics(const seastar::sstring &group_name) {
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

seastar::future<> manager::start(seastar::shared_ptr<service::storage_proxy> proxy_ptr, seastar::shared_ptr<gms::gossiper> gossiper_ptr) {
    _proxy_anchor = std::move(proxy_ptr);
    _gossiper_anchor = std::move(gossiper_ptr);

    co_await lister::scan_dir(_hints_dir, lister::dir_entry_types::of<directory_entry_type::directory>(),
            [this] (fs::path datadir, directory_entry de) {
        const locator::host_id host_id = locator::host_id{ utils::UUID{ de.name } };
        if (!check_dc_for(host_id)) {
            return seastar::make_ready_future<>();
        }
        return get_host_manager(host_id).populate_segments_to_replay();
    });
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

seastar::future<> manager::compute_hints_dir_device_id() {
    return get_device_id(_hints_dir.native()).then([this] (dev_t device_id) {
        _hints_dir_device_id = device_id;
    }).handle_exception([this] (auto eptr) {
        manager_logger.warn("Failed to stat directory {} for device id: {}", _hints_dir.native(), eptr);
        return make_exception_future<>(eptr);
    });
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

sync_point::shard_rps manager::calculate_current_sync_point(const std::vector<locator::host_id> &target_hosts) const {
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

seastar::future<> manager::wait_for_sync_point(seastar::abort_source &as, const sync_point::shard_rps &rps) {
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
    co_await seastar::coroutine::parallel_for_each(_host_managers, [&was_aborted, &rps, &local_as] (auto& p) {
        auto& [host_id, host_manager] = p;
        db::replay_position rp;

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
                return seastar::make_ready_future<>(std::current_exception());
            }

            return seastar::make_ready_future<>();
        });
    });

    if (was_aborted) {
        throw seastar::abort_requested_exception{};
    }
}

bool manager::host_hint_manager::store_hint(schema_ptr s, seastar::lw_shared_ptr<const frozen_mutation> fm,
        tracing::trace_state_ptr tr_state) noexcept
{
    try {
        (void) seastar::with_gate(_store_gate, [this, s = std::move(s), fm = std::move(fm), tr_state] () mutable {
            ++_hints_in_progress;
            size_t mut_size = fm->representation().size();
            shard_stats().size_of_hints_in_progress += mut_size;

            return seastar::with_shared(
                    file_update_mutex(),
                    seastar::coroutine::lambda([this, fm, s, tr_state] () mutable -> future<> {
                        try {
                            hints_store_ptr log_ptr = co_await get_or_load();
                            commitlog_entry_writer cew{s, *fm, db::commitlog::force_sync::no};

                            db::rp_handle rh = co_await log_ptr->add_entry(s->id(), cew,
                                    db::timeout_clock::now() + _shard_manager.hint_file_write_timeout);
                            auto rp = rh.release();
                            if (_last_written_rp < rp) {
                                _last_written_rp = rp;
                                manager_logger.debug("[{}] Updated last written replay position to {}", host_id(), rp);
                            }
                            ++shard_stats().written;

                            manager_logger.trace("Hint to {} has been stored", host_id());
                            tracing::trace(tr_state, "Hint to {} has been stored", host_id());
                        } catch (std::exception_ptr eptr) {
                            ++shard_stats().errors;

                            manager_logger.debug("store_hint(): got an exception when storing a hint to {}: {}", host_id(), eptr);
                            tracing::trace(tr_state, "Failed to store a hint to {}: {}", host_id(), eptr);
                        }
                    })
            ).finally([this, mut_size, fm, s] {
                --_hints_in_progress;
                shard_stats().size_of_hints_in_progress -= mut_size;
            });
        });
    } catch (...) {
        manager_logger.trace("Failed to store a hint to {}: {}", host_id(), std::current_exception());
        tracing::trace(tr_state, "Failed to store a hint to {}: {}", host_id(), std::current_exception());

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

void manager::host_hint_manager::start() {
    clear_stopped();
    allow_hints();
    _sender.start();
}

seastar::future<> manager::host_hint_manager::stop(drain should_drain) noexcept {
    if (stopped()) {
        throw std::logic_error(seastar::format("host_manager[{}]: stop() has been called twice", _host_id).c_str());
    }

    co_await seastar::async(seastar::coroutine::lambda([this, should_drain] () -> future<> {
        std::exception_ptr eptr;

        set_stopping();

        try {
            co_await _store_gate.close();
            co_await _sender.stop(should_drain);
        } catch (...) {
            eptr = std::current_exception();
        }

        /* co_await? */ (void) seastar::with_lock(file_update_mutex(), [this] {
            if (_hints_store_anchor) {
                hints_store_ptr tmp = std::exchange(_hints_store_anchor, nullptr);
                return tmp->shutdown().finally([tmp] {
                    return tmp->release();
                }).finally([tmp] {});
            }
            return seastar::make_ready_future<>();
        });

        if (eptr) {
            manager_logger.error("host_manager[{}]: exception: {}", _host_id, eptr);
        }

        set_stopped();
    }));
}

// TODO: Check the order if it makes sense. I've changed it compared to the original version.
manager::host_hint_manager::host_hint_manager(locator::host_id host_id, manager& shard_manager)
    : _host_id(host_id)
    , _sender(*this, _shard_manager.local_storage_proxy(), _shard_manager.local_db(), _shard_manager.local_gossiper())
    , _state(hint_manager_state_set::of<hint_manager_state::stopped>())
    , _shard_manager(shard_manager)
    , _file_update_mutex_ptr(seastar::lw_shared_ptr<seastar::shared_mutex>())
    , _file_update_mutex(*_file_update_mutex_ptr)
    , _last_written_rp(seastar::this_shard_id(), std::chrono::duration_cast<std::chrono::milliseconds>(runtime::get_boot_time().time_since_epoch()).count())
{}

// TODO: Ditto.
manager::host_hint_manager::host_hint_manager(host_hint_manager&& other)
    : _host_id(other._host_id)
    , _sender(other._sender, *this)
    , _state(other._state)
    , _hints_dir(std::move(other._hints_dir))
    , _shard_manager(other._shard_manager)
    , _file_update_mutex_ptr(std::move(other._file_update_mutex_ptr)) // ???? Why move?
    , _file_update_mutex(*_file_update_mutex_ptr)
    , _last_written_rp(other._last_written_rp)
{}

manager::host_hint_manager::~host_hint_manager() {
    assert(stopped());
}

seastar::future<hints_store_ptr> manager::host_hint_manager::get_or_load() {
    if (!_hints_store_anchor) {
        auto log_ptr = co_await _shard_manager.store_factory().get_or_load(_host_id, [this] (const locator::host_id&) noexcept {
            return add_store();
        });
        _hints_store_anchor = log_ptr;
        co_return log_ptr;
    }

    co_return _hints_store_anchor;
}

manager::host_hint_manager& manager::get_host_manager(locator::host_id host_id) {
    auto it = _host_managers.find(host_id);
    if (it == _host_managers.end()) {
        manager_logger.trace("Creating a hint_host_manager for {}", host_id);
        manager::host_hint_manager& hhman = _host_managers.emplace(host_id, host_hint_manager(host_id, *this)).first->second;
        hhman.start();
        return hhman;
    }
    return it->second;
}

bool manager::store_hint(locator::host_id host_id, schema_ptr s, seastar::lw_shared_ptr<const frozen_mutation> fm, tracing::trace_state_ptr tr_state) noexcept {
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
        manager_logger.trace("Failed to store a hint to {}: {}", host_id, std::current_exception);
        tracing::trace(tr_state, "Failed to store a hint to {}: {}", host_id, std::current_exception);

        ++_stats.errors;
        return false;
    }
}

seastar::future<db::commitlog> manager::host_hint_manager::add_store() noexcept {
    manager_logger.trace("Going to add a store to {}", _hints_dir.c_str());

    co_await seastar::futurize_invoke([this] {
        return io_check([name = _hints_dir.c_str()] { return recursive_touch_directory(name); }).then([this] {
            db::commitlog::config cfg;
            
            cfg.commit_log_location = _hints_dir.c_str();
            cfg.commitlog_segment_size_in_mb = resource_manager::hint_segment_size_in_mb;
            cfg.commitlog_total_space_in_mb = resource_manager::max_hints_per_ep_size_mb;
            cfg.fname_prefix = manager::FILENAME_PREFIX;
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

            return commitlog::create_commitlog(std::move(cfg)).then([this] (db::commitlog l) -> future<commitlog> {
                // add_store() is triggered every time hint files are forcefully flushed to I/O (every hints_flush_period).
                // When this happens we want to refill _sender's segments only if it has finished with the segments he had before.
                if (_sender.have_segments()) {
                    co_return l;
                }

                std::vector<sstring> segs_vec = co_await l.get_segments_to_replay();

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

                std::vector<std::pair<db::segment_id_type, sstring>> local_segs_vec;
                local_segs_vec.reserve(segs_vec.size());

                // Divide segments into those that were created on this shard
                // and those which were moved to it during rebalancing.
                for (auto& seg : segs_vec) {
                    db::commitlog::descriptor desc(seg, manager::FILENAME_PREFIX);
                    unsigned shard_id = db::replay_position(desc).shard_id();
                    if (shard_id == this_shard_id()) {
                        local_segs_vec.emplace_back(desc.id, std::move(seg));
                    } else {
                        _sender.add_foreign_segment(std::move(seg));
                    }
                }

                // Sort local segments by their segment ids, which should
                // correspond to the chronological order.
                std::sort(local_segs_vec.begin(), local_segs_vec.end());

                for (auto& [segment_id, seg] : local_segs_vec) {
                    _sender.add_segment(std::move(seg));
                }

                co_return l;
            });
        });
    });
}

seastar::future<> manager::host_hint_manager::flush_current_hints() noexcept {
    if (_hints_store_anchor) {
        co_await seastar::futurize_invoke([this] () -> seastar::future<> {
            co_await seastar::with_lock(file_update_mutex(), [this] () -> seastar::future<> {
                hints_store_ptr cptr = co_await get_or_load();
                
                co_await cptr->shutdown().finally([cptr] {
                    return cptr->release();
                }).finally([cptr] {});

                _hints_store_anchor = nullptr;
                co_await get_or_load().discard_result();
            });
        });
    }
}

class no_column_mapping : public std::out_of_range {
public:
    no_column_mapping(const table_schema_version& id)
        : std::out_of_range(seastar::format("column mapping for CF schema version {} is missing", id))
    {}
};

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

seastar::future<::timespec> manager::host_hint_manager::sender::get_last_file_modification(const seastar::sstring& fname) {
    file f = co_await seastar::open_file_dma(fname, seastar::open_flags::ro);
    const auto st = co_await seastar::do_with(std::move(f), [] (auto& f) {
        return f.stat();
    });
    co_return st.st_mtim;
}

seastar::future<> manager::host_hint_manager::sender::do_send_one_mutation(frozen_mutation_and_schema m,
        const inet_address_vector_replica_set& natural_endpoints) noexcept
{
    co_await seastar::futurize_invoke([this, m = std::move(m), &natural_endpoints] () mutable -> seastar::future<> {
        const std::optional<gms::inet_address> ep = _proxy.get_token_metadata_ptr()->get_endpoint_for_host_id(_host_id);
        if (ep && std::ranges::find(natural_endpoints, ep) != natural_endpoints.end()) {
            manager_logger.trace("Sending directly to {}", _host_id);
            co_await _proxy.send_hint_to_endpoint(std::move(m), ep.value());
        } else {
            manager_logger.trace("Endpoints set has changed and {} is no longer a replica. Mutating from scratch...", _host_id);
            co_await _proxy.send_hint_to_all_replicas(std::move(m));
        }
    });
}

bool manager::host_hint_manager::sender::can_send() noexcept {
    if (stopping() && !draining()) {
        return false;
    }

    try {
        const std::optional<gms::inet_address> ep = _proxy.get_token_metadata_ptr()->get_endpoint_for_host_id(_host_id);
        if (!ep.has_value()) [[unlikely]] {
            return false;
        }

        const auto ep_state_ptr = _gossiper.get_endpoint_state_for_endpoint_ptr(ep.value());
        if (ep_state_ptr && ep_state_ptr->is_alive()) {
            _state.remove(sender_state::host_left_ring);
            return true;
        } else {
            if (!_state.contains(sender_state::host_left_ring)) {
                _state.set_if<sender_state::host_left_ring>(!_shard_manager.local_db().get_token_metadata().is_normal_token_owner(ep.value()));
            }
            return _state.contains(sender_state::host_left_ring);
        }
    } catch (...) {
        return false;
    }
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
        return { freeze(m), std::move(schema) };
    }
    return { std::move(hr).mutation(), std::move(schema) };
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

bool manager::too_many_in_flight_hints_for(locator::host_id host_id) const noexcept {
    const auto my_host_id = _proxy_anchor->get_token_metadata_ptr()->get_my_id();
    const std::optional<gms::inet_address> maybe_ep = _proxy_anchor->get_token_metadata_ptr()->get_endpoint_for_host_id(host_id);
    
    if (!maybe_ep.has_value()) {
        return false;
    }

    return _stats.size_of_hints_in_progress > max_size_of_hints_in_progress &&
            host_id != my_host_id &&
            hints_in_progress_for(host_id) &&
            local_gossiper().get_endpoint_downtime(maybe_ep.value());
}

bool manager::can_hint_for(locator::host_id host_id) const noexcept {
    const auto my_host_id = _proxy_anchor->get_token_metadata_ptr()->get_my_id();
    if (host_id == my_host_id) {
        return false;
    }

    auto it = _host_managers.find(host_id);
    if (it != _host_managers.end() && (it->second.stopping() || !it->second.can_hint())) {
        return false;
    }

    const auto hipf = hints_in_progress_for(host_id);
    if (_stats.size_of_hints_in_progress > max_size_of_hints_in_progress && hipf > 0) {
        manager_logger.trace("size_of_hints_in_progress {} hints_in_progress_for({}) {}",
                _stats.size_of_hints_in_progress, host_id, hipf);
        return false;
    }

    if (!check_dc_for(host_id)) {
        manager_logger.trace("{}'s DC is not hintable", host_id);
        return false;
    }

    const std::optional<gms::inet_address> maybe_ep = _proxy_anchor->get_token_metadata_ptr()->get_endpoint_for_host_id(host_id);
    if (!maybe_ep.has_value()) {
        manager_logger.debug("Host of ID {} doesn't exist", host_id);
        return false;
    }

    const auto ep_downtime = local_gossiper().get_endpoint_downtime(maybe_ep.value());
    if (ep_downtime > _max_hint_window_us) {
        manager_logger.trace("{} is down for {}, not hinting", host_id, ep_downtime);
        return false;
    }

    return true;
}

seastar::future<> manager::change_host_filter(host_filter filter) {
    if (!started()) {
        throw std::logic_error{"change_host_filter: called before the hints_manager was started"};
    }

    co_await seastar::with_gate(_draining_hosts_gate, [this, filter = std::move(filter)] () mutable -> seastar::future<> {
        return seastar::with_semaphore(drain_lock(), 1, [this, filter = std::move(filter)] () mutable -> seastar::future<> {
            if (draining_all()) {
                throw std::logic_error{"change_host_filter: cannot change the configuration because all hints have been drained"};
            }

            manager_logger.debug("change_host_filter: changing from {} to {}", _host_filter, filter);
            std::swap(_host_filter, filter);

            const auto& topology = _proxy_anchor->get_token_metadata_ptr()->get_topology();
            
            try {
                co_await lister::scan_dir(_hints_dir, lister::dir_entry_types::of<directory_entry_type::directory>(),
                        [this, &topology] (fs::path datadir, seastar::directory_entry de) -> seastar::future<> {
                    const locator::host_id host_id = locator::host_id{utils::UUID{de.name}};

                    if (_host_managers.contains(host_id) || !_host_filter.can_hint_for(topology, host_id)) {
                        co_return;
                    }

                    co_await get_host_manager(host_id).populate_segments_to_replay();
                });
            } catch (...) {
                _host_filter = std::move(filter);
            }

            co_await seastar::coroutine::parallel_for_each(_host_managers, [this, &topology] (auto& pair) -> seastar::future<> {
                auto& [id, hmanager] = pair;
                
                if (_host_filter.can_hint_for(topology, id)) {
                    co_return;
                }

                co_await hmanager.stop(drain::no).handle_exception([] (auto) {/* ignore */});
                _host_managers.erase(id);
            });
        });
    });
}

bool manager::check_dc_for(locator::host_id host_id) const noexcept {
    const auto& topology = _proxy_anchor->get_token_metadata_ptr()->get_topology();
    try {
        return _host_filter.is_enabled_for_all() ||
                have_host_manager(host_id) ||
                _host_filter.can_hint_for(topology, host_id);
    } catch (...) {
        return false;
    }
}

void manager::drain_for(locator::host_id host_id) {
    if (!started() || stopping() || draining_all()) {
        return;
    }

    manager_logger.trace("on_leave_cluster: {} is removed/decommissioned", host_id);

    (void) seastar::with_gate(_draining_hosts_gate, [this, host_id] () -> seastar::future<> {
        co_await seastar::with_semaphore(drain_lock(), 1, [this, host_id] () -> seastar::future<> {
            try {
                co_await seastar::futurize_invoke([this, host_id] () -> seastar::future<> {
                    const auto my_host_id = _proxy_anchor->get_token_metadata_ptr()->get_my_id();
                    if (host_id == my_host_id) {
                        set_draining_all();

                        co_await seastar::coroutine::parallel_for_each(_host_managers, [] (auto& pair) -> seastar::future<> {
                            auto& [_, hmanager] = pair;
                            co_await hmanager.stop(drain::yes).handle_exception([] (auto) {/* ignore */});
                            co_await with_file_update_mutex(hmanager, [&hmanager = hmanager] {
                                return seastar::remove_file(hmanager.hints_dir().c_str());
                            });
                        });
                    } else { // if host_id != my_host_id
                        auto hmanager_it = _host_managers.find(host_id);
                        if (hmanager_it != _host_managers.end()) {
                            auto& [id, hmanager] = *hmanager_it;
                            co_await hmanager.stop(drain::yes).handle_exception([] (auto) {/* ignore */});
                            co_await with_file_update_mutex(hmanager, [&hmanager = hmanager] {
                                return seastar::remove_file(hmanager.hints_dir().c_str());
                            }).handle_exception([] (auto) {/* ignore */});
                            _host_managers.erase(host_id);
                        }
                    }
                });
            } catch (...) {
                manager_logger.error("Exception when draining {}: {}", host_id, std::current_exception());
            }
        });
    }).finally([host_id] {
        manager_logger.trace("drain_for: finished draining {}", host_id);
    });
}

manager::host_hint_manager::sender::sender(host_hint_manager& parent, service::storage_proxy& local_storage_proxy,
        replica::database& local_db, gms::gossiper& local_gossiper) noexcept
    : _stopped(seastar::make_ready_future<>())
    , _host_id(parent._host_id)
    , _host_manager(parent)
    , _shard_manager(_host_manager._shard_manager)
    , _resource_manager(_shard_manager._resource_manager)
    , _proxy(local_storage_proxy)
    , _db(local_db)
    , _hints_cpu_sched_group(_db.get_streaming_scheduling_group())
    , _gossiper(local_gossiper)
    , _file_update_mutex(_host_manager.file_update_mutex())
{}
manager::host_hint_manager::sender::sender(const sender& other, host_hint_manager& parent) noexcept
    : _stopped(make_ready_future<>())
    , _host_id(parent._host_id)
    , _host_manager(parent)
    , _shard_manager(_host_manager._shard_manager)
    , _resource_manager(_shard_manager._resource_manager)
    , _proxy(other._proxy)
    , _db(other._db)
    , _hints_cpu_sched_group(other._hints_cpu_sched_group)
    , _gossiper(other._gossiper)
    , _file_update_mutex(_host_manager.file_update_mutex())
{}

manager::host_hint_manager::sender::~sender() {
    dismiss_replay_waiters();
}

seastar::future<> manager::host_hint_manager::sender::stop(drain should_drain) noexcept {
    co_await seastar::async([this, should_drain] {
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

typename manager::host_hint_manager::sender::clock_type::duration manager::host_hint_manager::sender::next_sleep_duration() const {
    using duration_type = typename clock_type::duration;

    const time_point_type current_time = clock_type::now();
    const time_point_type next_flush_tp = std::max(_next_flush_tp, current_time);
    const time_point_type next_retry_tp = std::max(_next_send_retry_tp, current_time);

    const duration_type d = std::min(next_flush_tp, next_retry_tp) - current_time;

    return duration_type{10 * div_ceil(d.count(), 10)};
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

                sleep_abortable(next_sleep_duration(), _stop_as).get();
            } catch (const seastar::sleep_aborted&) {
                break;
            } catch (...) {
                manager_logger.trace("sender: got the exception: {}", std::current_exception());
            }
        }
    });
}

seastar::future<> manager::host_hint_manager::sender::send_one_mutation(frozen_mutation_and_schema m) {
    const auto& erm = _db.find_column_family(m.s).get_effective_replication_map();
    auto token = dht::get_token(*m.s, m.fm.key());
    inet_address_vector_replica_set natural_endpoints = erm->get_natural_endpoints(std::move(token));

    return do_send_one_mutation(std::move(m), natural_endpoints);
}

seastar::future<> manager::host_hint_manager::sender::send_one_hint(seastar::lw_shared_ptr<send_one_file_ctx> ctx_ptr,
        fragmented_temporary_buffer buf, db::replay_position rp, gc_clock::duration secs_since_file_mod,
        const seastar::sstring& fname)
{
    try {
        auto units = co_await _resource_manager.get_send_units_for(buf.size_bytes());
        ctx_ptr->mark_hint_as_in_progress(rp);

        (void) seastar::with_gate(ctx_ptr->file_send_gate,
                [this, secs_since_file_mod, &fname, buf = std::move(buf), rp, ctx_ptr] () mutable -> seastar::future<> {
            try {
                auto m = this->get_mutation(ctx_ptr, buf);
                const typename gc_clock::duration gc_grace_sec = m.s->gc_grace_seconds();

                if (gc_clock::now().time_since_epoch() - secs_since_file_mod > gc_grace_sec - manager::hints_flush_period) {
                    co_return;
                }

                try {
                    co_await send_one_mutation(std::move(m));
                    ++shard_stats().sent;
                } catch (...) {
                    manager_logger.trace("send_one_hint(): failed to send to {}: {}", _host_id, std::current_exception());
                    throw;
                }
            // ignore these errors and move on - probably this hint is too old and the KS/CF has been deleted...
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
                manager_logger.debug("send_hints(): unexpected error in file {} at {}: {}", fname, rp, std::current_exception());
                throw;
            }
        }).then_wrapped([this, units = std::move(units), rp, ctx_ptr] (seastar::future<>&& f) {
            if (!f.failed()) {
                ctx_ptr->on_hint_send_success(rp);
                auto new_bound = ctx_ptr->get_replayed_bound();
                if (new_bound.shard_id() == seastar::this_shard_id() && _sent_upper_bound_rp < new_bound) {
                    _sent_upper_bound_rp = new_bound;
                    notify_replay_waiters();
                }
            } else {
                ctx_ptr->on_hint_send_failure(rp);
            }
            f.ignore_ready_future();
        });
    } catch (...) {
        manager_logger.trace("send_one_file(): Hmm. Something bad has happened: {}", std::current_exception());
        ctx_ptr->on_hint_send_failure(rp);
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
        (*ptr) = std::nullopt;
        _replay_waiters.erase(rw_it);
    }
}

void manager::host_hint_manager::sender::dismiss_replay_waiters() noexcept {
    for (auto& p : _replay_waiters) {
        manager_logger.debug("[{}] dismiss_replay_waiters(): dismissing one", _host_id);
        auto ptr = p.second;
        (**ptr).set_exception(std::runtime_error{seastar::format("Hints manager for {} is stopping", _host_id)});
        (*ptr) = std::nullopt;
    }
    _replay_waiters.clear();
}

seastar::future<> manager::host_hint_manager::sender::wait_until_hints_are_replayed_up_to(seastar::abort_source &as,
        db::replay_position up_to_rp)
{
    manager_logger.debug("[{}] wait_until_hints_are_replayed_up_to(): entering with target {}", _host_id, up_to_rp);
    if (_foreign_segments_to_replay.empty() && up_to_rp < _sent_upper_bound_rp) {
        manager_logger.debug("[{}] wait_until_hints_are_replayed_up_to(): hints have already been replayed above the point ({} < {})",
                _host_id, up_to_rp, _sent_upper_bound_rp);
        co_return;
    }

    if (as.abort_requested()) {
        manager_logger.debug("[{}] wait_until_hints_are_replayed_up_to(): already aborted - stopping", _host_id);
        throw abort_requested_exception{};
    }

    auto ptr = seastar::make_lw_shared<std::optional<seastar::promise<>>>(seastar::promise<>{});
    auto it = _replay_waiters.emplace(up_to_rp, ptr);
    auto sub = as.subscribe([this, ptr, it] () noexcept {
        if (!ptr->has_value()) {
            return;
        }
        manager_logger.debug("[{}] wait_until_hints_are_replayed_up_to(): abort requested - stopping", _host_id);
        _replay_waiters.erase(it);
        (**ptr).set_exception(abort_requested_exception{});
    });

    co_await (**ptr).get_future().finally([sub = std::move(sub), host_id = _host_id] {
        manager_logger.debug("[{}] wait_until_hints_are_replayed_up_to(): returning after the future has been satisfied", host_id);
    });
}

void manager::host_hint_manager::sender::send_one_file_ctx::mark_hint_as_in_progress(db::replay_position rp) {
    in_progress_rps.insert(rp);
}

void manager::host_hint_manager::sender::send_one_file_ctx::on_hint_send_success(db::replay_position rp) noexcept {
    in_progress_rps.erase(rp);
    if (!last_succeeded_rp || *last_succeeded_rp < rp) {
        last_succeeded_rp = rp;
    }
}

void manager::host_hint_manager::sender::send_one_file_ctx::on_hint_send_failure(db::replay_position rp) noexcept {
    in_progress_rps.erase(rp);
    segment_replay_failed = true;
    if (!first_failed_rp || rp < *first_failed_rp) {
        first_failed_rp = rp;
    }
}

db::replay_position manager::host_hint_manager::sender::send_one_file_ctx::get_replayed_bound() const noexcept {
    // We are sure that all hints were sent _below_ the position which is the minimum of the following:
    // - Position of the first hint that failed to be sent in this replay (first_failed_rp),
    // - Position of the last hint which was successfully sent (last_succeeded_rp, inclusive bound),
    // - Position of the lowest hint which is being currently sent (in_progress_rps.begin()).

    db::replay_position rp;
    if (first_failed_rp) {
        rp = *first_failed_rp;
    } else if (last_succeeded_rp) {
        // It is always true that `first_failed_rp` <= `last_succeeded_rp`, so no need to compare
        rp = *last_succeeded_rp;
        // We replayed _up to_ `last_attempted_rp`, so the bound is not strict; we can increase `pos` by one
        rp.pos++;
    }

    if (!in_progress_rps.empty() && *in_progress_rps.begin() < rp) {
        rp = *in_progress_rps.begin();
    }

    return rp;
}

void manager::host_hint_manager::sender::rewind_sent_replay_position_to(db::replay_position rp) {
    _sent_upper_bound_rp = rp;
    notify_replay_waiters();
}

bool manager::host_hint_manager::sender::send_one_file(const seastar::sstring& fname) {
    timespec last_mod = get_last_file_modification(fname).get0();
    typename gc_clock::duration secs_since_file_mod = std::chrono::seconds(last_mod.tv_sec);
    seastar::lw_shared_ptr<send_one_file_ctx> ctx_ptr = seastar::make_lw_shared<send_one_file_ctx>(_last_schema_ver_to_column_mapping);

    try {
        commitlog::read_log_file(fname, manager::FILENAME_PREFIX, [this, secs_since_file_mod, &fname, ctx_ptr] (commitlog::buffer_and_replay_position buf_rp) -> future<> {
            auto& buf = buf_rp.buffer;
            auto& rp = buf_rp.position;

            while (true) {
                // Check that we can still send the next hint. Don't try to send it if the destination host
                // is DOWN or if we have already failed to send some of the previous hints.
                if (!draining() && ctx_ptr->segment_replay_failed) {
                    co_return;
                }

                // Break early if stop() was called or the destination node went down.
                if (!can_send()) {
                    ctx_ptr->segment_replay_failed = true;
                    co_return;
                }

                co_await flush_maybe();

                if (utils::get_local_injector().enter("hinted_handoff_pause_hint_replay")) {
                    // We cannot send the hint because hint replay is paused.
                    // Sleep 100ms and do the whole loop again.
                    //
                    // Jumping to the beginning of the loop makes sure that
                    // - We regularly check if we should stop - so that we won't
                    //   get stuck in shutdown.
                    // - flush_maybe() is called regularly - so that new segments
                    //   are created and we help enforce the "at most 10s worth of
                    //   hints in a segment".
                    co_await sleep(std::chrono::milliseconds(100));
                    continue;
                } else {
                    co_await send_one_hint(ctx_ptr, std::move(buf), rp, secs_since_file_mod, fname);
                    break;
                }
            };
        }, _last_not_complete_rp.pos, &_db.extensions()).get();
    } catch (db::commitlog::segment_error& ex) {
        manager_logger.error("{}: {}. Dropping...", fname, ex.what());
        ctx_ptr->segment_replay_failed = false;
        ++this->shard_stats().corrupted_files;
    } catch (...) {
        manager_logger.trace("sending of {} failed: {}", fname, std::current_exception());
        ctx_ptr->segment_replay_failed = true;
    }

    // wait till all background hints sending is complete
    ctx_ptr->file_send_gate.close().get();

    // If we are draining ignore failures and drop the segment even if we failed to send it.
    if (draining() && ctx_ptr->segment_replay_failed) {
        manager_logger.trace("send_one_file(): we are draining so we are going to delete the segment anyway");
        ctx_ptr->segment_replay_failed = false;
    }

    // update the next iteration replay position if needed
    if (ctx_ptr->segment_replay_failed) {
        // If some hints failed to be sent, first_failed_rp will tell the position of first such hint.
        // If there was an error thrown by read_log_file function itself, we will retry sending from
        // the last hint that was successfully sent (last_succeeded_rp).
        _last_not_complete_rp = ctx_ptr->first_failed_rp.value_or(ctx_ptr->last_succeeded_rp.value_or(_last_not_complete_rp));
        manager_logger.trace("send_one_file(): error while sending hints from {}, last RP is {}", fname, _last_not_complete_rp);
        return false;
    }

    // If we got here we are done with the current segment and we can remove it.
    with_shared(_file_update_mutex, [&fname, this] {
        auto p = _host_manager.get_or_load().get0();
        return p->delete_segments({ fname });
    }).get();

    // clear the replay position - we are going to send the next segment...
    _last_not_complete_rp = replay_position();
    _last_schema_ver_to_column_mapping.clear();
    manager_logger.trace("send_one_file(): segment {} was sent in full and deleted", fname);
    return true;
}

const seastar::sstring* manager::host_hint_manager::sender::name_of_current_segment() const noexcept {
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
    } catch (...) {
        manager_logger.trace("send_hints(): got the exception: {}", std::current_exception());
    }

    if (have_segments()) {
        // TODO: come up with something more sophisticated here
        _next_send_retry_tp = clock_type::now() + std::chrono::seconds(1);
    } else {
        // if there are no segments to send we want to retry when we maybe have some (after flushing)
        _next_send_retry_tp = _next_flush_tp;
    }

    manager_logger.trace("send_hints(): we have handled {} segments", replayed_segments_count);
}

// TODO: Couldn't this be templated so that we can avoid using std::function and, consequently, allocating memory?
// Look where exactly this function is called.
static future<> scan_for_hints_dirs(const seastar::sstring& hints_directory,
        std::function<future<> (fs::path dir, directory_entry de, unsigned shard_id)> f)
{
    co_await lister::scan_dir(hints_directory, lister::dir_entry_types::of<directory_entry_type::directory>(),
            [f = std::move(f)] (fs::path dir, directory_entry de) mutable {
        unsigned shard_id;
        try {
            shard_id = std::stoi(de.name.c_str());
        } catch (std::invalid_argument& ex) {
            manager_logger.debug("Ignore invalid directory {}", de.name);
            return seastar::make_ready_future<>();
        }
        return f(std::move(dir), std::move(de), shard_id);
    });
}

manager::hints_segment_map manager::get_current_hints_segments(const seastar::sstring& hints_directory) {
    hints_segment_map current_hints_segments;

    scan_for_hints_dirs(hints_directory, [&current_hints_segments] (fs::path dir, directory_entry de, unsigned shard_id) {
        manager_logger.trace("shard_id = {}", shard_id);

        return lister::scan_dir(dir / de.name.c_str(), lister::dir_entry_types::of<directory_entry_type::directory>(),
                [&current_hints_segments, shard_id] (fs::path dir, directory_entry de) {
            manager_logger.trace("\tHost ID: {}", de.name);
            const auto host_id = locator::host_id{utils::UUID{de.name}};

            return lister::scan_dir(dir / de.name.c_str(), lister::dir_entry_types::of<directory_entry_type::regular>(),
                    [&current_hints_segments, shard_id, host_id] (fs::path dir, directory_entry de) {
                manager_logger.trace("\t\tFile: {}", de.name);
                current_hints_segments[host_id][shard_id].emplace_back(dir / de.name.c_str());
                return seastar::make_ready_future<>();
            });
        });
    }).get();

    return current_hints_segments;
}

void manager::rebalance_segments(const seastar::sstring& hints_directory, hints_segment_map& segment_map) {
    std::unordered_map<locator::host_id, size_t> hints_per_host;

    for (const auto& [host_id, host_hints_map] : segment_map) {
        hints_per_host[host_id] = std::transform_reduce(
                host_hints_map.begin(), host_hints_map.end(),
                size_t{0}, std::plus<>(),
                [] (const auto& pair) { return pair.second.size(); }
        );

        manager_logger.trace("{}: total files: {}", host_id, hints_per_host[host_id]);
    }

    std::unordered_map<
}

} // namespace db::hints
