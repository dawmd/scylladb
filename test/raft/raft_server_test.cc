#include <fmt/std.h>
#include "raft/raft.hh"
#include "replication.hh"
#include "seastar/core/abort_source.hh"
#include "seastar/testing/thread_test_case.hh"
#include "utils/error_injection.hh"
#include <seastar/util/defer.hh>

#ifdef SEASTAR_DEBUG
// Increase tick time to allow debug to process messages
 const auto tick_delay = 200ms;
#else
const auto tick_delay = 100ms;
#endif

SEASTAR_THREAD_TEST_CASE(test_check_abort_on_client_api) {
    raft_cluster<std::chrono::steady_clock> cluster(
            test_case { .nodes = 1 },
            [](raft::server_id id, const std::vector<raft::command_cref>& commands, lw_shared_ptr<hasher_int> hasher) {
                return 0;
            },
            0,
            0,
            0, false, tick_delay, rpc_config{});
    cluster.start_all().get();

    cluster.stop_server(0, "test crash").get();

    auto check_error = [](const raft::stopped_error& e) {
        return sstring(e.what()) == sstring("Raft instance is stopped, reason: \"test crash\"");
    };
    BOOST_CHECK_EXCEPTION(cluster.add_entries(1, 0).get(), raft::stopped_error, check_error);
    BOOST_CHECK_EXCEPTION(cluster.get_server(0).modify_config({}, {to_raft_id(0)}, nullptr).get(), raft::stopped_error, check_error);
    BOOST_CHECK_EXCEPTION(cluster.get_server(0).read_barrier(nullptr).get(), raft::stopped_error, check_error);
    BOOST_CHECK_EXCEPTION(cluster.get_server(0).set_configuration({}, nullptr).get(), raft::stopped_error, check_error);
}

SEASTAR_THREAD_TEST_CASE(test_release_memory_if_add_entry_throws) {
#ifndef SCYLLA_ENABLE_ERROR_INJECTION
    std::cerr << "Skipping test as it depends on error injection. Please run in mode where it's enabled (debug,dev).\n";
#else
    const size_t command_size = sizeof(size_t);
    raft_cluster<std::chrono::steady_clock> cluster(
            test_case {
                .nodes = 1,
                .config = std::vector<raft::server::configuration>({
                    raft::server::configuration {
                        .snapshot_threshold_log_size = 0,
                        .snapshot_trailing_size = 0,
                        .max_log_size = command_size,
                        .max_command_size = command_size
                    }
                })
            },
            ::apply_changes,
            0,
            0,
            0, false, tick_delay, rpc_config{});
    cluster.start_all().get();
    auto stop = defer([&cluster] { cluster.stop_all().get(); });

    utils::get_local_injector().enable("fsm::add_entry/test-failure", true);
    auto check_error = [](const std::runtime_error& e) {
        return sstring(e.what()) == sstring("fsm::add_entry/test-failure");
    };
    BOOST_CHECK_EXCEPTION(cluster.add_entries(1, 0).get(), std::runtime_error, check_error);

    // we would block forever if the memory wasn't released
    // when the exception was thrown from the first add_entry
    cluster.add_entries(1, 0).get();
    cluster.read(read_value{0, 1}).get();
#endif
}

// This test considers the simplest case of aborting operations on raft::server.
// It verifies the following things:
//
// * Triggering the passed abort_source does abort the operation.
// * The corresponding futures throw raft::request_aborted.
//
// We only cover the tests that aren't responsible for changing the state.
SEASTAR_THREAD_TEST_CASE(test_aborting_raft_operations) {
    const size_t command_size = sizeof(size_t);
    raft_cluster<std::chrono::steady_clock> cluster(
            test_case {
                .nodes = 3,
                .config = std::vector<raft::server::configuration>({
                    raft::server::configuration {
                        .snapshot_threshold_log_size = 0,
                        .snapshot_trailing_size = 0,
                        .max_log_size = command_size,
                        .max_command_size = command_size
                    }
                })
            },
            ::apply_changes,
            0,
            0,
            0, false, tick_delay, rpc_config{});
    cluster.start_all().get();
    auto stop = defer([&cluster] { cluster.stop_all().get(); });

    const size_t server_id = 0;
    auto& server = cluster.get_server(server_id);

    // The operations below, e.g. read_barrier, need to go through the leader.
    // We lose the leadership and isolate server 0 so that the corresponding
    // futures don't resolve immediately. Thanks to this, aborting the operations
    // will result in an exception as intended.
    //
    // Note that the isolated part of the cluster holds the quorum, so we won't
    // be able to make progress.
    cluster.elect_new_leader(1).get();
    cluster.isolate(::isolate {.id = server_id}).get();

    auto do_test = [&server] <typename Func> (Func func) {
        abort_source as;
        as.request_abort();
        auto fut = std::invoke(func, server, &as);
        BOOST_CHECK_THROW((void) fut.get(), raft::request_aborted);
    };

    do_test(&raft::server::read_barrier);
    do_test(&raft::server::wait_for_state_change);

    // TODO: These do not work out of the box. Extend the test to handle them too.
    // do_test(&raft::server::wait_for_leader);
    // do_test(&raft::server::trigger_snapshot);

    // For a clean shutdown.
    cluster.connect_all();
}
