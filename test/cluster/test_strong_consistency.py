#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import asyncio
from test.pylib.manager_client import ManagerClient
from test.pylib.util import gather_safely, wait_for
from test.cluster.util import new_test_keyspace
from test.pylib.internal_types import ServerInfo
from cassandra.protocol import InvalidRequest

import pytest
import logging
import time
import uuid


logger = logging.getLogger(__name__)


async def wait_for_leader(manager: ManagerClient, s: ServerInfo, group_id: str):
    async def get_leader_host_id():
        result = await manager.api.get_raft_leader(s.ip_addr, group_id)
        return None if uuid.UUID(result).int == 0 else result
    return await wait_for(get_leader_host_id, time.time() + 60)


@pytest.mark.asyncio
async def test_basic_write_read(manager: ManagerClient):

    logger.info("Bootstrapping cluster")
    config = {
        'experimental_features': ['strongly-consistent-tables']
    }
    cmdline = [
        '--logger-log-level', 'sc_groups_manager=debug',
        '--logger-log-level', 'sc_coordinator=debug'
    ]
    servers = await manager.servers_add(3, config=config, cmdline=cmdline, auto_rack_dc='my_dc')
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
async def test_timed_out_read(manager: ManagerClient):
    config = {
        "experimental_features": ["strongly-consistent-tables"]
    }
    cmdline = [
        "--logger-log-level", "sc_groups_manager=debug",
        "--logger-log-level", "sc_coordinator=debug"
    ]

    s1 = await manager.server_add(config=config, cmdline=cmdline)
    cql, _ = await manager.get_ready_cql([s1])

    log = await manager.server_open_log(s1.server_id)
    mark = await log.mark()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1} AND consistency = 'local'") as ks:
        table_name = "my_table"
        table = f"{ks}.{table_name}"

        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, v int)")
        await cql.run_async(f"INSERT INTO {table} (pk, v) VALUES (0, 13)")
        await manager.api.enable_injection(s1.ip_addr, "sc_coordinator_wait_before_query_read_barrier", one_shot=False)

        fut = cql.run_async(f"SELECT * FROM {table} WHERE pk = 0 USING TIMEOUT 1s")
        await log.wait_for("sc_coordinator_wait_before_query_read_barrier", from_mark=mark, timeout=10)

        await asyncio.sleep(1)
        await manager.api.message_injection(s1.ip_addr, "sc_coordinator_wait_before_query_read_barrier")

        with pytest.raises(Exception, match="Operation timed out"):
            await fut

        # Sanity check: Nothing broke and we can still read from the table.
        res = await cql.run_async(f"SELECT * FROM {table} WHERE pk = 0")
        assert res[0].v == 13

@pytest.mark.asyncio
async def test_abort_raft_operations(manager: ManagerClient):
    logger.info("Bootstrapping cluster")
    config = {
        "experimental_features": ["strongly-consistent-tables"]
    }
    cmdline = [
        "--logger-log-level", "sc_groups_manager=debug",
        "--logger-log-level", "sc_coordinator=debug"
    ]

    s1 = await manager.server_add(config=config, cmdline=cmdline)
    cql, _ = await manager.get_ready_cql([s1])

    log = await manager.server_open_log(s1.server_id)
    mark = await log.mark()

    async with new_test_keyspace(manager, "WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1} AND tablets = {'initial': 1} AND consistency = 'local'") as ks:
        table_name = "my_table"
        table = f"{ks}.{table_name}"

        await cql.run_async(f"CREATE TABLE {table} (pk int PRIMARY KEY, v int)")

        table_id = await manager.get_table_id(ks, table_name)
        rows = await cql.run_async(f"SELECT raft_group_id FROM system.tablets WHERE table_id = {table_id}")
        group_id = str(rows[0].raft_group_id)

        logger.info(f"Get current leader for the group {group_id}")
        leader_host_id = await wait_for_leader(manager, s1, group_id)

        await manager.api.enable_injection(s1.ip_addr, "sc_coordinator_wait_before_query_read_barrier", one_shot=False)

        async def f():
            await asyncio.sleep(30)
            await manager.server_stop(s1.server_id)

        t = asyncio.create_task(f())

        fut = cql.run_async(f"SELECT * FROM {table} WHERE pk = 0 USING TIMEOUT 10s")
        await log.wait_for("sc_coordinator_wait_before_query_read_barrier", from_mark=mark, timeout=10)
        # assert False

        await cql.run_async(f"DROP TABLE {table}")
        # await manager.api.disable_injection(s1.ip_addr, "sc_coordinator_wait_before_query_read_barrier")
        await manager.api.message_injection(s1.ip_addr, "sc_coordinator_wait_before_query_read_barrier")
        await fut
        await t
