#
# Copyright (C) 2023-present ScyllaDB
#
# SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
#

import asyncio
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra import Unauthorized
from cassandra.connection import UnixSocketEndPoint
from test.cluster.conftest import cluster_con, skip_mode
from test.pylib.manager_client import ManagerClient

import pytest
from test.cluster.auth_cluster import extra_scylla_config_options as auth_config


@pytest.mark.asyncio
async def test_maintenance_socket(manager: ManagerClient):
    """
    Test that when connecting to the maintenance socket, the user has superuser permissions,
    even if the authentication is enabled on the regular port.
    """
    config = {
        **auth_config,
        "authenticator": "PasswordAuthenticator",
        "authorizer": "CassandraAuthorizer",
    }

    server = await manager.server_add(config=config)
    socket = await manager.server_get_maintenance_socket_path(server.server_id)

    try:
        cluster = Cluster([server.ip_addr])
        cluster.connect()
    except NoHostAvailable:
        pass
    else:
        pytest.fail("Client should not be able to connect if auth provider is not specified")

    cluster = cluster_con([server.ip_addr],
                          auth_provider=PlainTextAuthProvider(username="cassandra", password="cassandra"))
    session = cluster.connect()

    session.execute("CREATE ROLE john WITH PASSWORD = 'password' AND LOGIN = true;")
    session.execute("CREATE KEYSPACE ks1 WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};")
    session.execute("CREATE KEYSPACE ks2 WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};")
    session.execute("CREATE TABLE ks1.t1 (pk int PRIMARY KEY, val int);")
    session.execute("CREATE TABLE ks2.t1 (pk int PRIMARY KEY, val int);")
    session.execute("GRANT SELECT ON ks1.t1 TO john;")

    cluster = cluster_con([server.ip_addr], auth_provider=PlainTextAuthProvider(username="john", password="password"))
    session = cluster.connect()
    try:
        session.execute("SELECT * FROM ks2.t1")
    except Unauthorized:
        pass
    else:
        pytest.fail("User 'john' has no permissions to access ks2.t1")

    maintenance_cluster = cluster_con([UnixSocketEndPoint(socket)])
    maintenance_session = maintenance_cluster.connect()

    # check that the maintenance session has superuser permissions
    maintenance_session.execute("SELECT * FROM ks1.t1")
    maintenance_session.execute("SELECT * FROM ks2.t1")
    maintenance_session.execute("INSERT INTO ks1.t1 (pk, val) VALUES (1, 1);")
    maintenance_session.execute("CREATE KEYSPACE ks3 WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};")
    maintenance_session.execute("CREATE TABLE ks1.t2 (pk int PRIMARY KEY, val int);")


@pytest.mark.asyncio
@skip_mode("release", "error injections are not supported in release mode")
async def test_basic_operations_with_not_fully_initialized_node(manager: ManagerClient):
    """
    Verify that basic operations via the maintenenance socket don't lead to
    the node crashing even when performed before the node is fully initialized.

    We cover a few different scenarios to cover as much relevant code as possible:
    * `LIST EFFECTIVE SERVICE LEVEL`: service level controller + basic part of auth
    * `DESCRIBE SCHEMA WITH INTERNALS`: service level controller (basic) + auth (full)
    * `INSERT INTO`, `SELECT`: basic operations on a table.

    Note that it's not explicitly documented what should happen with a partially
    initialized Scylla. However, since the CQL port is already exposed, we assume
    it's expected to get requests via the maintenance socket at that point onwards.

    We skip operations like `CREATE ...` or `ALTER ...` because modifying the schema
    is not possible, at least with a partially initialized Scylla. At the moment,
    it crashes at any attempt to do so.
    """

    error_injection = "pause_after_setting_up_maintenance_socket"

    config = {
        **auth_config,
        "authenticator": "PasswordAuthenticator",
        "authorizer": "CassandraAuthorizer",
    }
    config_err = config | {"error_injections_at_startup": [error_injection]}

    srv = await manager.server_add(config=config)
    cql = manager.get_cql()

    await cql.run_async("CREATE ROLE bob WITH PASSWORD = 'mypassword' AND LOGIN = true")
    await cql.run_async("CREATE SERVICE LEVEL sl1 WITH SHARES = 150")
    await cql.run_async("ATTACH SERVICE LEVEL sl1 TO bob")

    replication_opts = "replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
    await cql.run_async(f"CREATE KEYSPACE ks WITH {replication_opts}")
    await cql.run_async("CREATE TABLE ks.my_table (pk int PRIMARY KEY, v int)")

    await manager.server_stop_gracefully(srv.server_id)
    await manager.server_update_config(srv.server_id, config_options=config_err)

    start_task = asyncio.create_task(manager.server_start(srv.server_id))
    log = await manager.server_open_log(srv.server_id)
    # Wait for the maintenance socket to be set up.
    await log.wait_for(error_injection)

    socket = await manager.server_get_maintenance_socket_path(srv.server_id)
    maintenance_cluster = cluster_con([UnixSocketEndPoint(socket)])
    maintenance_session = maintenance_cluster.connect()

    with pytest.raises(Exception, match="operation not supported through maintenance socket"):
        maintenance_session.execute("LIST EFFECTIVE SERVICE LEVEL OF bob")

    with pytest.raises(Exception, match="operation not supported through maintenance socket"):
        maintenance_session.execute("DESCRIBE SCHEMA WITH INTERNALS")

    maintenance_session.execute("INSERT INTO ks.my_table (pk, v) VALUES (1, 2)")
    maintenance_session.execute("UPDATE ks.my_table SET v = 3 WHERE pk = 1")

    row = maintenance_session.execute(f"SELECT * FROM ks.my_table").one()
    assert row.pk == 1 and row.v == 3

    await manager.api.message_injection(srv.ip_addr, error_injection)
    await start_task
