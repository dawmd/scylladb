#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

from test.pylib.manager_client import ManagerClient
from test.pylib.util import gather_safely, wait_for, Host
from test.cluster.util import new_test_keyspace
from test.pylib.internal_types import HostID, ServerInfo
from cassandra.protocol import InvalidRequest

import asyncio
import pytest
import logging
import time
import uuid

from typing import Tuple

logger = logging.getLogger(__name__)

DEFAULT_CONFIG = {'experimental_features': ['strongly-consistent-tables']}
DEFAULT_CMDLINE = [
        '--logger-log-level', 'sc_groups_manager=debug',
        '--logger-log-level', 'sc_coordinator=debug'
    ]


async def wait_for_leader(manager: ManagerClient, s: ServerInfo, group_id: str):
    async def get_leader_host_id():
        result = await manager.api.get_raft_leader(s.ip_addr, group_id)
        return None if uuid.UUID(result).int == 0 else result
    return await wait_for(get_leader_host_id, time.time() + 60)


@pytest.mark.asyncio
async def test_basic_write_read(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    servers = await manager.servers_add(3, config=DEFAULT_CONFIG, cmdline=DEFAULT_CMDLINE, auto_rack_dc='my_dc')
    (cql, hosts) = await manager.get_ready_cql(servers)

    logger.info("Load host_id-s for servers")
    host_ids = await gather_safely(*[manager.get_host_id(s.server_id) for s in servers])

    def host_by_host_id(host_id):
        for hid, host in zip(host_ids, hosts):
            if hid == host_id:
                return host
        raise RuntimeError(f"Can't find host for host_id {host_id}")

    logger.info("Creating a strongly-consistent keyspace")
    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 1} AND consistency = 'local'") as ks:
        logger.info("Creating a table")
        await cql.run_async(f"CREATE TABLE {ks}.test (pk int PRIMARY KEY, c int);")

        logger.info("Select raft group id for the tablet")
        table_id = await manager.get_table_id(ks, 'test')
        rows = await cql.run_async(f"SELECT raft_group_id FROM system.tablets where table_id = {table_id}")
        group_id = str(rows[0].raft_group_id)

        logger.info(f"Get current leader for the group {group_id}")
        leader_host_id = await wait_for_leader(manager, servers[0], group_id)
        leader_host = host_by_host_id(leader_host_id)
        non_leader_host = next((host_by_host_id(hid) for hid in host_ids if hid != leader_host_id), None)
        assert non_leader_host is not None

        logger.info(f"Run INSERT statement on leader {leader_host}")
        await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES (10, 20)", host=leader_host)
        logger.info(f"Run INSERT statement on non-leader {non_leader_host}")
        with pytest.raises(InvalidRequest, match="Strongly consistent writes can be executed only on the leader node"):
            await cql.run_async(f"INSERT INTO {ks}.test (pk, c) VALUES (10, 30)", host=non_leader_host)

        logger.info(f"Run SELECT statement on leader {leader_host}")
        rows = await cql.run_async(f"SELECT * FROM {ks}.test WHERE pk = 10;", host=leader_host)
        assert len(rows) == 1
        row = rows[0]
        assert row.pk == 10
        assert row.c == 20

        logger.info(f"Run SELECT statement on non-leader {non_leader_host}")
        rows = await cql.run_async(f"SELECT * FROM {ks}.test WHERE pk = 10;", host=non_leader_host)
        assert len(rows) == 1
        row = rows[0]
        assert row.pk == 10
        assert row.c == 20

        # Check that we can restart a server with an active tablets raft group
        await manager.server_restart(servers[2].server_id)

    # To check that the servers can be stopped gracefully. By default the test runner just kills them.
    await gather_safely(*[manager.server_stop_gracefully(s.server_id) for s in servers])

@pytest.mark.asyncio
@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
@pytest.mark.parametrize("error_injection_name", [
    "sc_coordinator_wait_before_acquire_server",
    "sc_coordinator_wait_before_query_read_barrier"])
async def test_timed_out_read(manager: ManagerClient, error_injection_name: str):
    """
    A simple test verifying that we don't get stuck for an indefinite amount
    of time while reading from a strongly consistent table. As soon as the
    deadline for a query ends, the operation should be canceled and a time-out
    exception should be returned.

    This test focuses on a Raft operation being the potential reason for
    getting stuck. It should be aborted when we reach the deadline.
    """

    s1 = await manager.server_add(config=DEFAULT_CONFIG, cmdline=DEFAULT_CMDLINE)
    cql, _ = await manager.get_ready_cql([s1])

    log = await manager.server_open_log(s1.server_id)
    mark = await log.mark()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1} AND consistency = 'local'") as ks:
        table_name = "my_table"
        table = f"{ks}.{table_name}"

        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, v int)")
        await cql.run_async(f"INSERT INTO {table} (pk, v) VALUES (0, 13)")
        await manager.api.enable_injection(s1.ip_addr, error_injection_name, one_shot=True)

        timeout = 1

        read_fut = cql.run_async(f"SELECT * FROM {table} WHERE pk = 0 USING TIMEOUT {timeout}s")
        await log.wait_for(error_injection_name, from_mark=mark)

        # The sleep will take AT LEAST 1 second. Since we've already hit the error
        # injection, the operation will have timed out when we wake up.
        await asyncio.sleep(timeout)
        await manager.api.message_injection(s1.ip_addr, error_injection_name)

        # FIXME: Once Scylla throws a more proper exception type, adapt this to it.
        with pytest.raises(Exception, match="Operation timed out"):
            await read_fut

        # Sanity check: Nothing broke and we can still read from the table.
        res = await cql.run_async(f"SELECT * FROM {table} WHERE pk = 0")
        assert res[0].v == 13

@pytest.mark.asyncio
@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
@pytest.mark.parametrize("error_injection_name", [
    "sc_coordinator_wait_before_acquire_server",
    "sc_coordinator_wait_before_begin_mutate",
    "sc_coordinator_wait_before_add_entry"])
async def test_timed_out_write(manager: ManagerClient, error_injection_name: str):
    """
    A simple test verifying that we don't get stuck for an indefinite amount
    of time while writing to a strongly consistent table. As soon as the
    deadline for a query ends, the operation should be canceled and a time-out
    exception should be returned.

    This test focuses on a Raft operation being the potential reason for
    getting stuck. It should be aborted when we reach the deadline.
    """

    s1 = await manager.server_add(config=DEFAULT_CONFIG, cmdline=DEFAULT_CMDLINE)
    cql, _ = await manager.get_ready_cql([s1])

    log = await manager.server_open_log(s1.server_id)
    mark = await log.mark()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1} AND consistency = 'local'") as ks:
        table_name = "my_table"
        table = f"{ks}.{table_name}"

        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, v int)")
        await manager.api.enable_injection(s1.ip_addr, error_injection_name, one_shot=True)

        timeout = 1

        write_fut = cql.run_async(f"INSERT INTO {table} (pk, v) VALUES (0, 13) USING TIMEOUT {timeout}s")
        await log.wait_for(error_injection_name, from_mark=mark)

        # The sleep will take AT LEAST 1 second. Since we've already hit the error
        # injection, the operation will have timed out when we wake up.
        await asyncio.sleep(timeout)
        await manager.api.message_injection(s1.ip_addr, error_injection_name)

        # FIXME: Once Scylla throws a more proper exception type, adapt this to it.
        with pytest.raises(Exception, match="Operation timed out"):
            await write_fut

        # Sanity check: Nothing broke and we can still write to the table.
        await cql.run_async(f"INSERT INTO {table} (pk, v) VALUES (0, 7)")

@pytest.mark.asyncio
@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_aborted_read(manager: ManagerClient):
    """
    A simple test verifying that pending reads to a strongly consistent
    table are canceled if the table has been dropped in the meantime.

    The test focuses on a Raft operation as being the potential reason
    for the read to get stuck or being slow enough that the table can
    be dropped during its execution.
    """

    s1 = await manager.server_add(config=DEFAULT_CONFIG, cmdline=DEFAULT_CMDLINE)
    cql, _ = await manager.get_ready_cql([s1])

    log = await manager.server_open_log(s1.server_id)
    mark = await log.mark()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1} AND consistency = 'local'") as ks:
        table_name = "my_table"
        table = f"{ks}.{table_name}"

        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, v int)")
        await cql.run_async(f"INSERT INTO {table} (pk, v) VALUES (0, 13)")
        await manager.api.enable_injection(s1.ip_addr, "sc_coordinator_wait_before_query_read_barrier", one_shot=True)

        fut = cql.run_async(f"SELECT * FROM {table} WHERE pk = 0")
        await log.wait_for("sc_coordinator_wait_before_query_read_barrier", from_mark=mark)

        mark = await log.mark()

        await cql.run_async(f"DROP TABLE {table}")

        # Sanity check: The table was really dropped and we can no longer read from it.
        with pytest.raises(InvalidRequest, match="unconfigured table"):
            await cql.run_async(f"SELECT * FROM {table} WHERE pk = 0")

        # Wait for the Raft group to be removed. This should happen almost immediately
        # after dropping the table, but let's avoid any potential casue of flakiness.
        await log.wait_for(r"schedule_raft_group_deletion\(\): raft server for group id \S+ aborted", from_mark=mark)

        await manager.api.message_injection(s1.ip_addr, "sc_coordinator_wait_before_query_read_barrier")
        with pytest.raises(Exception, match="Raft group is being removed. Retry the operation"):
            await fut

@pytest.mark.asyncio
@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_aborted_write(manager: ManagerClient):
    """
    A simple test verifying that pending writes to a strongly consistent
    table are canceled if the table has been dropped in the meantime.

    The test focuses on a Raft operation as being the potential reason
    for the write to get stuck or being slow enough that the table can
    be dropped during its execution.
    """

    s1 = await manager.server_add(config=DEFAULT_CONFIG, cmdline=DEFAULT_CMDLINE)
    cql, _ = await manager.get_ready_cql([s1])

    log = await manager.server_open_log(s1.server_id)
    mark = await log.mark()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1} AND consistency = 'local'") as ks:
        table_name = "my_table"
        table = f"{ks}.{table_name}"

        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, v int)")
        await manager.api.enable_injection(s1.ip_addr, "sc_coordinator_wait_before_add_entry", one_shot=True)

        fut = cql.run_async(f"INSERT INTO {table} (pk, v) VALUES (0, 13)")
        await log.wait_for("sc_coordinator_wait_before_add_entry", from_mark=mark)

        mark = await log.mark()

        await cql.run_async(f"DROP TABLE {table}")

        # Sanity check: The table was really dropped and we can no longer write to it.
        with pytest.raises(InvalidRequest, match="unconfigured table"):
            await cql.run_async(f"INSERT INTO {table} (pk, v) VALUES (0, 11)")

        # Wait for the Raft group to be removed. This should happen almost immediately
        # after dropping the table, but let's avoid any potential casue of flakiness.
        await log.wait_for(r"schedule_raft_group_deletion\(\): raft server for group id \S+ aborted", from_mark=mark)

        await manager.api.message_injection(s1.ip_addr, "sc_coordinator_wait_before_add_entry")
        with pytest.raises(Exception, match="Raft group is being removed. Retry the operation"):
            await fut

@pytest.mark.asyncio
@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_read_when_shutting_down(manager: ManagerClient):
    """
    A simple test verifying that pending reads are canceled when the node
    starts shutting down.

    The potential cause of hanging we're testing here is being stuck
    at a Raft operation when reading from a strongly consistent table.
    """

    config = DEFAULT_CONFIG | {"request_timeout_on_shutdown_in_seconds": 1}
    cmdline = DEFAULT_CMDLINE + ["--logger-log-level", "cql_server=debug"]

    servers = await manager.servers_add(3, config=config, cmdline=cmdline, auto_rack_dc="dc1")
    cql, hosts = await manager.get_ready_cql(servers)

    async def pick_leader_info(host_id: HostID) -> Tuple[Host, ServerInfo]:
        for host, server in zip(hosts, servers):
            srv_host_id = await manager.get_host_id(server.server_id)
            if srv_host_id == host_id:
                return (host, server)
        raise RuntimeError(f"Can't find host for host_id {host_id}")

    async def get_leader(keyspace: str, table: str) -> Tuple[Host, ServerInfo]:
        logger.info("Select raft group id for the tablet")
        table_id = await manager.get_table_id(keyspace, table)
        rows = await cql.run_async(f"SELECT raft_group_id FROM system.tablets WHERE table_id = {table_id}")
        group_id = str(rows[0].raft_group_id)

        logger.info(f"Get current leader for the group {group_id}")
        leader_host_id = await wait_for_leader(manager, servers[0], group_id)

        logger.info(f"Leader of group {group_id} is {leader_host_id}")

        leader_host, leader_info = await pick_leader_info(leader_host_id)
        logger.info(f"Further information on leader of group {group_id}: server_id={leader_info.server_id}, ip={leader_info.ip_addr}")

        return (leader_host, leader_info)

    prevent_read_injection = "sc_coordinator_wait_before_query_read_barrier"

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 1} AND consistency = 'local'") as ks:
        table_name = "my_table"
        table = f"{ks}.{table_name}"

        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, v int)")
        leader_host, leader_info = await get_leader(ks, table_name)

        log = await manager.server_open_log(leader_info.server_id)
        mark = await log.mark()

        await manager.api.enable_injection(leader_info.ip_addr, prevent_read_injection, one_shot=True)

        read_fut = cql.run_async(f"SELECT * FROM {table} WHERE pk = 0", host=leader_host)
        await log.wait_for(prevent_read_injection, from_mark=mark)
        mark = await log.mark()

        stop_fut = asyncio.create_task(manager.server_stop_gracefully(leader_info.server_id))

        await log.wait_for("generic_server::shutdown completed", from_mark=mark)
        await manager.api.message_injection(leader_info.ip_addr, prevent_read_injection)

        # Throw some more queries at the node. Currently, these all should end up
        # with a failure (pretty much immediately), but it's better to have some
        # sanity check here in case there's some dormant bug: if there is some
        # problem, these ongoing reads might prevent the node from shutting down.
        await asyncio.gather(*[cql.run_async(f"SELECT * FROM {table} WHERE pk = 0") for _ in range(100)])

        # We cannot predict what the result of the query is going to be.
        # Technically, we could ensure either outcome, but it requires
        # playing with more error injections. We don't really care about
        # the result (the test just wants to make sure we stop the node
        # fast enough), so let's just log it and call it a day.
        try:
            await read_fut
            logger.debug("The read ended successfully")
        except Exception as e:
            logger.debug(f"The read ended in an exception: {e}")
            pass

        await stop_fut

@pytest.mark.asyncio
@pytest.mark.skip_mode(mode="release", reason="error injections are not supported in release mode")
async def test_write_when_shutting_down(manager: ManagerClient):
    """
    A simple test verifying that pending writes are canceled when the node
    starts shutting down.

    The potential cause of hanging we're testing here is being stuck
    at a Raft operation when writign to a strongly consistent table.
    """

    config = DEFAULT_CONFIG | {"request_timeout_on_shutdown_in_seconds": 1}
    cmdline = DEFAULT_CMDLINE + ["--logger-log-level", "cql_server=debug"]

    servers = await manager.servers_add(3, config=config, cmdline=cmdline, auto_rack_dc="dc1")
    cql, hosts = await manager.get_ready_cql(servers)

    async def pick_leader_info(host_id: HostID) -> Tuple[Host, ServerInfo]:
        for host, server in zip(hosts, servers):
            srv_host_id = await manager.get_host_id(server.server_id)
            if srv_host_id == host_id:
                return (host, server)
        raise RuntimeError(f"Can't find host for host_id {host_id}")

    async def get_leader(keyspace: str, table: str) -> Tuple[Host, ServerInfo]:
        logger.info("Select raft group id for the tablet")
        table_id = await manager.get_table_id(keyspace, table)
        rows = await cql.run_async(f"SELECT raft_group_id FROM system.tablets WHERE table_id = {table_id}")
        group_id = str(rows[0].raft_group_id)

        logger.info(f"Get current leader for the group {group_id}")
        leader_host_id = await wait_for_leader(manager, servers[0], group_id)

        logger.info(f"Leader of group {group_id} is {leader_host_id}")

        leader_host, leader_info = await pick_leader_info(leader_host_id)
        logger.info(f"Further information on leader of group {group_id}: server_id={leader_info.server_id}, ip={leader_info.ip_addr}")

        return (leader_host, leader_info)

    prevent_write_injection = "sc_coordinator_wait_before_add_entry"

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3} AND tablets = {'initial': 1} AND consistency = 'local'") as ks:
        table_name = "my_table"
        table = f"{ks}.{table_name}"

        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, v int)")
        leader_host, leader_info = await get_leader(ks, table_name)

        log = await manager.server_open_log(leader_info.server_id)
        mark = await log.mark()

        await manager.api.enable_injection(leader_info.ip_addr, prevent_write_injection, one_shot=True)

        write_fut = cql.run_async(f"INSERT INTO {table} (pk, v) VALUES (0, 13)", host=leader_host)
        await log.wait_for(prevent_write_injection, from_mark=mark)
        mark = await log.mark()

        stop_fut = asyncio.create_task(manager.server_stop_gracefully(leader_info.server_id))

        await log.wait_for("generic_server::shutdown completed", from_mark=mark)
        await manager.api.message_injection(leader_info.ip_addr, prevent_write_injection)

        # Throw some more queries at the node. Currently, these all should end up
        # with a failure (pretty much immediately), but it's better to have some
        # sanity check here in case there's some dormant bug: if there is some
        # problem, these ongoing reads might prevent the node from shutting down.
        #
        # It would be more natural to send writes here, but the leader is going
        # to change, and at the moment we must send writes directly to the leader.
        await asyncio.gather(*[cql.run_async(f"SELECT * FROM {table} WHERE pk = 0") for _ in range(100)])

        # We cannot predict what the result of the query is going to be.
        # Technically, we could ensure either outcome, but it requires
        # playing with more error injections. We don't really care about
        # the result (the test just wants to make sure we stop the node
        # fast enough), so let's just log it and call it a day.
        try:
            await write_fut
            logger.debug("The write ended successfully")
        except Exception as e:
            logger.debug(f"The write ended in an exception: {e}")
            pass

        await stop_fut
