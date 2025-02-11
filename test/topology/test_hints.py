#
# Copyright (C) 2025-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#


import asyncio
import pytest

from test.pylib.manager_client import ManagerClient

from cassandra.query import SimpleStatement, ConsistencyLevel

from test.topology.conftest import skip_mode
from test.topology.util import new_test_keyspace, new_test_table
from test.topology_custom.test_hints import create_sync_point, await_sync_point

@pytest.mark.asyncio
async def test_draining_hints(manager: ManagerClient):
    """
    This test verifies that all hints are drained when a node is being decommissioned.
    """

    s1, s2, _ = await manager.servers_add(3)
    cql = manager.get_cql()

    await manager.api.set_logger_level(s1.ip_addr, "hints_manager", "trace")

    async with new_test_keyspace(cql, "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}") as ks:
        async with new_test_table(cql, ks, "pk int PRIMARY KEY, v int") as t:
            await manager.server_stop_gracefully(s2.server_id)

            # Generate hints towards s2 on s1 with probability 1 - ((#nodes - 1) / #nodes)^1000 ~= 1.
            for i in range(1000):
                await cql.run_async(SimpleStatement(f"INSERT INTO {t} (pk, v) VALUES ({i}, {i + 1})", consistency_level=ConsistencyLevel.ANY))

            sync_point = create_sync_point(s1)
            await manager.server_start(s2.server_id)

            async def wait():
                assert await_sync_point(s1, sync_point, 60)

            async with asyncio.TaskGroup() as tg:
                _ = tg.create_task(wait())
                _ = tg.create_task(manager.decommission_node(s1.server_id, 60))

@pytest.mark.asyncio
@skip_mode("release", "error injections are not supported in release mode")
async def test_canceling_hint_draining(manager: ManagerClient):
    """
    This test verifies that draining hints is canceled as soon as we issue a shutdown.
    """

    s1, s2, _ = await manager.servers_add(3)
    cql = manager.get_cql()

    await manager.api.set_logger_level(s1.ip_addr, "hints_manager", "trace")

    async with new_test_keyspace(cql, "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}") as ks:
        async with new_test_table(cql, ks, "pk int PRIMARY KEY, v int") as t:
            await manager.server_stop_gracefully(s2.server_id)

            # Generate hints towards s2 on s1 with probability 1 - ((#nodes - 1) / #nodes)^1000 ~= 1.
            for i in range(1000):
                await cql.run_async(SimpleStatement(f"INSERT INTO {t} (pk, v) VALUES ({i}, {i + 1})", consistency_level=ConsistencyLevel.ANY))

            await manager.api.enable_injection(s1.ip_addr, "hinted_handoff_pause_hint_replay", False, {})
            await manager.remove_node(s1.server_id, s2.server_id)
            await manager.server_stop_gracefully(s1.server_id)
