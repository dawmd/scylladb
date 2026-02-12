/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "coordinator.hh"
#include "exceptions/exceptions.hh"
#include "schema/schema.hh"
#include "replica/database.hh"
#include "locator/tablet_replication_strategy.hh"
#include "service/strong_consistency/state_machine.hh"
#include "service/strong_consistency/groups_manager.hh"
#include "idl/strong_consistency/state_machine.dist.hh"
#include "idl/strong_consistency/state_machine.dist.impl.hh"
#include "utils/error_injection.hh"

namespace service::strong_consistency {


static logging::logger logger("sc_coordinator");

static const locator::tablet_replica* find_replica(const locator::tablet_info& tinfo, locator::host_id id) {
    const auto it = std::ranges::find_if(tinfo.replicas,
        [&] (const locator::tablet_replica& r) {
            return r.host == id;
        });
    return it == tinfo.replicas.end() ? nullptr : &*it;
}

struct coordinator::operation_ctx {
    locator::effective_replication_map_ptr erm;
    raft_server raft_server;
    locator::tablet_id tablet_id;
    const locator::tablet_raft_info& raft_info;
    const locator::tablet_info& tablet_info;
};

auto coordinator::create_operation_ctx(const schema& schema, const dht::token& token) 
    -> future<value_or_redirect<operation_ctx>>
{
    auto erm = schema.table().get_effective_replication_map();
    if (const auto* tablet_aware_rs = erm->get_replication_strategy().maybe_as_tablet_aware();
        !tablet_aware_rs || 
        tablet_aware_rs->get_consistency() != data_dictionary::consistency_config_option::local)
    {
        on_internal_error(logger,
            format("Unexpected replication strategy '{}' with consistency '{}' for table {}.{}",
                erm->get_replication_strategy().get_type(),
                tablet_aware_rs
                    ? consistency_config_option_to_string(tablet_aware_rs->get_consistency())
                    : "<undefined>",
                schema.ks_name(), schema.cf_name()));
    }
    const auto this_replica = locator::tablet_replica {
        .host = erm->get_token_metadata().get_my_id(),
        .shard = this_shard_id()
    };
    const auto& tablet_map = erm->get_token_metadata().tablets().get_tablet_map(schema.id());
    const auto tablet_id = tablet_map.get_tablet_id(token);
    const auto& tablet_info = tablet_map.get_tablet_info(tablet_id);

    if (!contains(tablet_info.replicas, this_replica)) {
        const auto* target = find_replica(tablet_info, this_replica.host);
        co_return need_redirect{target ? *target : tablet_info.replicas.at(0)};
    }
    const auto& raft_info = tablet_map.get_tablet_raft_info(tablet_id);
    auto raft_server = co_await _groups_manager.acquire_server(raft_info.group_id);

    co_return operation_ctx {
        .erm = std::move(erm),
        .raft_server = std::move(raft_server),
        .tablet_id = tablet_id,
        .raft_info = raft_info,
        .tablet_info = tablet_info
    };
}

coordinator::coordinator(groups_manager& groups_manager, replica::database& db)
    : _groups_manager(groups_manager)
    , _db(db)
{
}

future<value_or_redirect<>> coordinator::mutate(schema_ptr schema,
        const dht::token& token,
        mutation_gen&& mutation_gen)
{
    int idx = 0;
    while (true) {
        logger.info("mutate(): Loop start");
        if (idx++ >= 3) {
            on_internal_error(logger, "Hit the limit of iterations");
        }
        //! This can also indirectly block on Raft!
        logger.info("mutate(): Step 1");
        auto op_result = co_await create_operation_ctx(*schema, token);
        if (const auto* redirect = get_if<need_redirect>(&op_result)) {
            logger.info("mutate(): Step 1: redirect");
            co_return *redirect;
        }
        logger.info("mutate(): Step 2");
        auto& op = get<operation_ctx>(op_result);

        while (true) {
            logger.info("mutate(): Step 2.1: Inner loop start");
            co_await utils::get_local_injector().inject("sc_coordinator_wait_before_begin_mutate",
                utils::wait_for_message(5min));
            logger.info("mutate(): Step 2.2");
                
            //! This can also indirectly block on Raft!
            auto disposition = op.raft_server.begin_mutate();
            if (const auto* not_a_leader = get_if<raft::not_a_leader>(&disposition)) {
                const auto leader_host_id = locator::host_id{not_a_leader->leader.uuid()};
                const auto* target = find_replica(op.tablet_info, leader_host_id);
                if (!target) {
                    on_internal_error(logger,
                        ::format("table {}.{}, tablet {}, current leader {} is not a replica, replicas {}",
                            schema->ks_name(), schema->cf_name(), op.tablet_id, 
                            leader_host_id, op.tablet_info.replicas));
                }
                logger.info("mutate(): Step 2.2.1: redirect: {}", *target);
                co_return need_redirect{*target};
            }
            logger.info("mutate(): Step 2.3");
            if (auto* wait_for_leader = get_if<raft_server::need_wait_for_leader>(&disposition)) {
                logger.info("mutate(): Step 2.3.1");
                co_await std::move(wait_for_leader->future);
                logger.info("mutate(): Step 2.3.2");
                continue;
            }
            logger.info("mutate(): Step 2.4");
            const auto [ts, term] = get<raft_server::timestamp_with_term>(disposition);

            const raft_command command {
                .mutation{mutation_gen(ts)}
            };
            raft::command raft_cmd;
            ser::serialize(raft_cmd, command);

            logger.debug("mutate(): add_entry({}), term {}",
                command.mutation.pretty_printer(schema), term);
            auto& group_state = op.raft_server._state;

            co_await utils::get_local_injector().inject("sc_coordinator_wait_before_adding_entry",
                    utils::wait_for_message(5min));
            logger.info("mutate(): Step 2.5");

            try {
                co_await op.raft_server.server().add_entry(std::move(raft_cmd),
                    raft::wait_type::committed,
                    &group_state.as);
                logger.info("mutate(): Step 2.6.1");
                logger.debug("mutate(): add_entry finished, returning monostate");
                co_return std::monostate{};
            } catch (...) {
                auto ex = std::current_exception();
                if (try_catch<raft::request_aborted>(ex)) {
                    // The abort_source may be triggered because of the node
                    // shutting down or the group being removed.
                    // Either situation is within expectations.
                    logger.debug("mutate(): add_entry, operation aborted {}, table {}.{}, tablet {}, term {}",
                        ex, schema->ks_name(), schema->cf_name(), op.tablet_id, term);
                    // No matter which case it is, the effective replication map might've
                    // changed, so we need to obtain a fresh context for this operation.
                    //
                    // Trying to simply retry the operation in the next iteration
                    // would likely produce the same result; we might actually get
                    // stuck in a deadlock.
                    //
                    // Unfortunately, without tablet migration, there's very little
                    // we can do now.
                    //
                    // FIXME: Retry with the new leader.
                    //
                    // ACTUALLY, since Raft is already implemented, chances are that
                    // the new leader has already been elected and everything's OK
                    // with it.
                    // break;
                    throw exceptions::server_exception(
                        "The operation was aborted due to internal reasons. "
                        "Retrying the statement may be necessary.");
                } else if (try_catch<raft::stopped_error>(ex)) {
                    // Holding raft_server.holder guarantees that the raft::server is not
                    // aborted until the holder is released.

                    on_internal_error(logger,
                        format("mutate(): add_entry, unexpected exception {}, table {}.{}, tablet {}, term {}", 
                            ex, schema->ks_name(), schema->cf_name(), op.tablet_id, term));
                } else if (try_catch<raft::not_a_leader>(ex) || try_catch<raft::dropped_entry>(ex)) {
                    logger.debug("mutate(): add_entry, got retriable error {}, table {}.{}, tablet {}, term {}",
                        ex, schema->ks_name(), schema->cf_name(), op.tablet_id, term);

                    continue;
                } else if (try_catch<raft::commit_status_unknown>(ex)) {
                    logger.debug("mutate(): add_entry, got commit_status_unknown {}, table {}.{}, tablet {}, term {}",
                        ex, schema->ks_name(), schema->cf_name(), op.tablet_id, term);

                    // FIXME: use a dedicated ERROR_CODE instead of SERVER_ERROR
                    throw exceptions::server_exception(
                        "The outcome of this statement is unknown. It may or may not have been applied. "
                        "Retrying the statement may be necessary.");
                }

                logger.debug("mutate(): add_entry, unknown exception {}, table {}.{}, tablet {}, term {}",
                    ex, schema->ks_name(), schema->cf_name(), op.tablet_id, term);
                // We know nothing about other errors, let the cql server convert them to SERVER_ERROR.
                throw;
            }
            logger.info("mutate(): Step 2.5: Inner loop end");
        }
        logger.info("mutate(): Step 3: Loop end");
    }
}

auto coordinator::query(schema_ptr schema,
        const query::read_command& cmd,
        const dht::partition_range_vector& ranges,
        tracing::trace_state_ptr trace_state,
        db::timeout_clock::time_point timeout
    ) -> future<query_result_type>
{
    //! This can also indirectly block on Raft!
    auto op_result = co_await create_operation_ctx(*schema, ranges[0].start()->value().token());
    if (const auto* redirect = get_if<need_redirect>(&op_result)) {
        co_return *redirect;
    }
    auto& op = get<operation_ctx>(op_result);
    auto& group_state = op.raft_server._state;

    auto aoe = abort_on_expiry(timeout);
    auto sub = group_state.as.subscribe([&aoe] noexcept {
        aoe.abort_source().request_abort();
    });

    co_await utils::get_local_injector().inject("sc_coordinator_wait_before_query_read_barrier",
            utils::wait_for_message(5min));
    logger.info("query(): after the error injection");

    try {
        co_await op.raft_server.server().read_barrier(&aoe.abort_source());
        logger.info("query(): read_barrier finished: ok");
    } catch (const raft::request_aborted& ex) {
        logger.debug("query(): read_barrier aborted [table {}.{}, tablet {}]. Command: {}. Reason: {}",
            schema->ks_name(), schema->cf_name(), op.tablet_id, cmd, ex);

        if (timeout > db::timeout_clock::now()) {
            throw exceptions::server_exception("The query was aborted due to internal reasons. "
                "Try retrying the statement.");
        } else {
            // FIXME: Use a dedicated exception type for strongly consistent tables.
            throw exceptions::server_exception("Operation timed out");
        }
    }

    logger.info("Trying query db");
    auto [result, cache_temp] = co_await _db.query(schema, cmd,
        query::result_options::only_result(), ranges, trace_state, timeout);
    logger.info("Querying db ok");

    co_return std::move(result);
}

}
