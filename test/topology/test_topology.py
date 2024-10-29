#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
"""
Test consistency of schema changes with topology changes.
"""
import pytest
import logging
import asyncio
import random
import re
import time

from test.pylib.util import wait_for

from test.pylib.scylla_cluster import ReplaceConfig
from test.pylib.manager_client import ManagerClient
from cassandra.query import SimpleStatement

from cassandra import ConsistencyLevel                  # type: ignore

logger = logging.getLogger(__name__)


# @pytest.mark.asyncio
# async def test_add_server_add_column(manager, random_tables):
#     """Add a node and then add a column to a table and verify"""
#     table = await random_tables.add_table(ncolumns=5)
#     await manager.server_add()
#     await table.add_column()
#     await random_tables.verify_schema()


# @pytest.mark.asyncio
# async def test_stop_server_add_column(manager, random_tables):
#     """Add a node, stop an original node, add a column"""
#     servers = await manager.running_servers()
#     table = await random_tables.add_table(ncolumns=5)
#     await manager.server_add()
#     await manager.server_stop(servers[1].server_id)
#     await table.add_column()
#     await random_tables.verify_schema()


# @pytest.mark.asyncio
# async def test_restart_server_add_column(manager, random_tables):
#     """Add a node, stop an original node, add a column"""
#     servers = await manager.running_servers()
#     table = await random_tables.add_table(ncolumns=5)
#     ret = await manager.server_restart(servers[1].server_id)
#     await table.add_column()
#     await random_tables.verify_schema()


# @pytest.mark.asyncio
# async def test_remove_node_add_column(manager, random_tables):
#     """Add a node, remove an original node, add a column"""
#     servers = await manager.running_servers()
#     table = await random_tables.add_table(ncolumns=5)
#     await manager.server_add()
#     await manager.server_stop_gracefully(servers[1].server_id)              # stop     [1]
#     await manager.remove_node(servers[0].server_id, servers[1].server_id)   # Remove   [1]
#     await table.add_column()
#     await random_tables.verify_schema()
#     # TODO: check that group 0 no longer contains the removed node (#12153)


# @pytest.mark.asyncio
# async def test_decommission_node_add_column(manager, random_tables):
#     """Add a node, remove an original node, add a column"""
#     table = await random_tables.add_table(ncolumns=5)
#     servers = await manager.running_servers()
#     decommission_target = servers[1]
#     # The sleep injections significantly increase the probability of reproducing #11780:
#     # 1. bootstrapped_server finishes bootstrapping and enters NORMAL state
#     # 2. decommission_target starts storage_service::handle_state_normal(bootstrapped_server),
#     #    enters sleep before calling storage_service::notify_joined
#     # 3. we start decommission on decommission_target
#     # 4. decommission_target sends node_ops_verb with decommission_prepare request to bootstrapped_server
#     # 5. bootstrapped_server receives the RPC and enters sleep
#     # 6. decommission_target handle_state_normal wakes up,
#     #    calls storage_service::notify_joined which drops some RPC clients
#     # 7. If #11780 is not fixed, this will fail the node_ops_verb RPC, causing decommission to fail
#     await manager.api.enable_injection(
#         decommission_target.ip_addr, 'storage_service_notify_joined_sleep', one_shot=True)
#     bootstrapped_server = await manager.server_add()
#     async def no_joining_nodes():
#         joining_nodes = await manager.api.get_joining_nodes(decommission_target.ip_addr)
#         return not joining_nodes
#     # Wait until decommission_target thinks that bootstrapped_server is NORMAL
#     # note: when this wait finishes, we're usually in the middle of storage_service::handle_state_normal
#     await wait_for(no_joining_nodes, time.time() + 30, period=.1)
#     await manager.api.enable_injection(
#         bootstrapped_server.ip_addr, 'storage_service_decommission_prepare_handler_sleep', one_shot=True)
#     await manager.decommission_node(decommission_target.server_id)
#     await table.add_column()
#     await random_tables.verify_schema()
#     # TODO: check that group 0 no longer contains the decommissioned node (#12153)

# @pytest.mark.asyncio
# async def test_replace_different_ip(manager: ManagerClient, random_tables) -> None:
#     servers = await manager.running_servers()
#     await manager.server_stop(servers[0].server_id)
#     replace_cfg = ReplaceConfig(replaced_id = servers[0].server_id, reuse_ip_addr = False)
#     await manager.server_add(replace_cfg)
#     # TODO: check that group 0 no longer contains the replaced node (#12153)

# @pytest.mark.asyncio
# async def test_replace_reuse_ip(manager: ManagerClient, random_tables) -> None:
#     servers = await manager.running_servers()
#     await manager.server_stop(servers[0].server_id)
#     replace_cfg = ReplaceConfig(replaced_id = servers[0].server_id, reuse_ip_addr = True)
#     await manager.server_add(replace_cfg)
#     # TODO: check that group 0 no longer contains the replaced node (#12153)


# @pytest.mark.asyncio
# @pytest.mark.skip(reason="Wait for @slow attribute, #11713")
# async def test_remove_node_with_concurrent_ddl(manager, random_tables):
#     stopped = False
#     ddl_failed = False

#     async def do_ddl():
#         nonlocal ddl_failed
#         iteration = 0
#         while not stopped:
#             logger.debug(f'ddl, iteration {iteration} started')
#             try:
#                 # If the node was removed, the driver may retry "create table" on another node,
#                 # but the request might have already been completed.
#                 # The same applies to drop_table.

#                 await random_tables.add_tables(5, 5, if_not_exists=True)
#                 await random_tables.verify_schema()
#                while len(random_tables.tables) > 0:
#                     await random_tables.drop_table(random_tables.tables[-1], if_exists=True)
#                 logger.debug(f'ddl, iteration {iteration} finished')
#             except:
#                 logger.exception(f'ddl, iteration {iteration} failed')
#                 ddl_failed = True
#                 raise
#             iteration += 1

#     async def do_remove_node():
#         for i in range(10):
#             logger.debug(f'do_remove_node [{i}], iteration started')
#             if ddl_failed:
#                 logger.debug(f'do_remove_node [{i}], ddl failed, exiting')
#                 break
#             server_ids = await manager.running_servers()
#             host_ids = await asyncio.gather(*(manager.get_host_id(s) for s in server_ids))
#             initiator_index, target_index = random.sample(range(len(server_ids)), 2)
#             initiator_ip = server_ids[initiator_index]
#             target_ip = server_ids[target_index]
#             target_host_id = host_ids[target_index]
#             logger.info(f'do_remove_node [{i}], running remove_node, '
#                         f'initiator server [{initiator_ip}], target ip [{target_ip}], '
#                         f'target host id [{target_host_id}]')
#             await manager.wait_for_host_known(initiator_ip, target_host_id)
#             logger.info(f'do_remove_node [{i}], stopping target server [{target_ip}], host_id [{target_host_id}]')
#             await manager.server_stop_gracefully(target_ip)
#             logger.info(f'do_remove_node [{i}], target server [{target_ip}] stopped, '
#                         f'waiting for it to be down on [{initiator_ip}]')
#             await manager.wait_for_host_down(initiator_ip, target_ip)
#             logger.info(f'do_remove_node [{i}], invoking remove_node')
#             await manager.remove_node(initiator_ip, target_ip, target_host_id)
#             # TODO: check that group 0 no longer contains the removed node (#12153)
#             logger.info(f'do_remove_node [{i}], remove_node done')
#             new_server_ip = await manager.server_add()
#             logger.info(f'do_remove_node [{i}], server_add [{new_server_ip}] done')
#             logger.info(f'do_remove_node [{i}], iteration finished')

#     ddl_task = asyncio.create_task(do_ddl())
#     try:
#         await do_remove_node()
#     finally:
#         logger.debug("do_remove_node finished, waiting for ddl fiber")
#         stopped = True
#         await ddl_task
#         logger.debug("ddl fiber done, finished")

@pytest.mark.asyncio
async def test_mv_broken_promises(manager: ManagerClient):
    print("COS idzie")
    servers = await manager.running_servers()
    assert len(servers) == 3

    cql = manager.cql

    [s1, s2, s3] = servers

    await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}")
    await cql.run_async("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)")
    await cql.run_async("CREATE MATERIALIZED VIEW ks.t_view AS SELECT pk, v FROM ks.t WHERE v IS NOT NULL PRIMARY KEY (v, pk)")

    await manager.server_stop_gracefully(s3.server_id)
    await asyncio.sleep(10)

    alive_servers = [s1, s2]

    # logs = [await manager.server_open_log(server.server_id) for server in alive_servers]
    # logs = [(log, await log.mark()) for log in logs]

    print("PRZED forem")

    for i in range(10000):
        await cql.run_async(SimpleStatement(f"INSERT INTO ks.t (pk, v) VALUES ({i}, {i + 1})", consistency_level=ConsistencyLevel.ONE))
    
    print("PO forze")
    
    await manager.server_stop_gracefully(s1.server_id)

    await manager.remove_node(s1.server_id, s3.server_id)

    print("PO remove")

    await asyncio.sleep(5)

    await manager.server_stop_gracefully(s1.server_id)

    # pattern = re.compile(r"Error applying view update .* \(view: ks\.t_view, .*\): seastar::broken_promise")
    # results = [await log.grep(pattern, from_mark=mark) for log, mark in logs]
    # # Flatten the list of lists.
    # results = sum(results, [])

    # assert results != []

    # for server in servers:
    #     await manager.server_stop_gracefully(server.server_id)
    #     await manager.
    # [server1, server2, server3] = await manager.servers_add(servers_num=3, )

    # cql = manager.get_cql()

    # await cql.run_async("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}")
    # await cql.run_async("CREATE TABLE ks.t (pk int PRIMARY KEY, v int)")
    # await cql.run_async("CREATE MATERIALIZED VIEW ks.t_view AS SELECT pk, v FROM ks.t WHERE v IS NOT NULL PRIMARY KEY (v, pk)")

    # await manager.server_stop_gracefully(server3.server_id)

    # alive_servers = [server1, server2]

    # for server in alive_servers:
    #     # Prevent saving any hints.
    #     await manager.api.enable_injection(server.ip_addr, "reject_incoming_hints", one_shot=False)
    #     # There are two things to note at this point:
    #     # (i)  MV updates use CL=ANY,
    #     # (ii) when CL=ANY is used when applying a write, consistency level is achieved even if
    #     #      no node responds, but a hint has been written.
    #     # Because we disable saving hints in the previous step, if no node responds, consistency level
    #     # will NOT be achieved and an error should be set. This error injection prevents that.
    #     await manager.api.enable_injection(server.ip_addr, "abstract_write_response_handler_no_targets_dont_set_failure", one_shot=False)

    # logs = [await manager.server_open_log(server.server_id) for server in alive_servers]
    # logs = [(log, await log.mark()) for log in logs]

    # for i in range(100):
    #     await cql.run_async(SimpleStatement(f"INSERT INTO ks.t (pk, v) VALUES ({i}, {i + 1})", consistency_level=ConsistencyLevel.ONE))

    # # Trigger a topology change. This will ensure that all writes that have been issued up to this point
    # # will either need to finish or time out before this operation can end.
    # await manager.remove_node(server1.server_id, server3.server_id, timeout=30)

    # pattern = re.compile(r"Error applying view update .* \(view: ks\.t_view, .*\): seastar::broken_promise")
    # results = [await log.grep(pattern, from_mark=mark) for log, mark in logs]
    # # Flatten the list of lists.
    # results = sum(results, [])

    # assert results != []

