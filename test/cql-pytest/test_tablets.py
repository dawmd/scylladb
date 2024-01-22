# Copyright 2023-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

#############################################################################
# Some tests for the new "tablets"-based replication, replicating the old
# "vnodes". Eventually, ScyllaDB will use tablets by default and all tests
# will run using tablets, but these tests are for specific issues discovered
# while developing tablets that didn't exist for vnodes. Note that most tests
# for tablets require multiple nodes, and are in the test/topology*
# directory, so here we'll probably only ever have a handful of single-node
# tests.
#############################################################################

import pytest
from util import unique_name, new_test_keyspace
from cassandra.protocol import ConfigurationException, InvalidRequest

# A fixture similar to "test_keyspace", just creates a keyspace that enables
# tablets with initial_tablets=128
# The "initial_tablets" feature doesn't work if the "tablets" experimental
# feature is not turned on; In such a case, the tests using this fixture
# will be skipped.
@pytest.fixture(scope="module")
def test_keyspace_128_tablets(cql, this_dc):
    name = unique_name()
    try:
        cql.execute("CREATE KEYSPACE " + name + " WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', '" + this_dc + "': 1 } AND TABLETS = { 'enabled': true, 'initial': 128 }")
    except ConfigurationException:
        pytest.skip('Scylla does not support initial_tablets, or the tablets feature is not enabled')
    yield name
    cql.execute("DROP KEYSPACE " + name)

# In the past (issue #16493), repeatedly creating and deleting a table
# would leak memory. Creating a table with 128 tablets would make this
# leak 128 times more serious and cause a failure faster. This is a
# reproducer for this problem. We basically expect this test not to
# OOM Scylla - the test doesn't "check" anything, the way it failed was
# for Scylla to run out of memory and then fail one of the CREATE TABLE
# or DROP TABLE operations in the loop.
# Note that this test doesn't even involve any data inside the table.
# Reproduces #16493.
def test_create_loop_with_tablets(cql, test_keyspace_128_tablets):
    table = test_keyspace_128_tablets + "." + unique_name()
    for i in range(100):
        cql.execute(f"CREATE TABLE {table} (p int PRIMARY KEY, v int)")
        cql.execute("DROP TABLE " + table)

# We want to ensure that we can only change the RF of any DC by at most 1 at a time
# if we use tablets. That provides us with the guarantee that the old and the new QUORUM
# overlap by at least one node.
def test_alter_tablet_keyspace(cql, this_dc):
    with new_test_keyspace(cql, f"WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{this_dc}' : 1 }} "
                                f"AND TABLETS = {{ 'enabled': true, 'initial': 128 }}") as keyspace:
        def change_opt_rf(rf_opt, new_rf):
            cql.execute(f"ALTER KEYSPACE {keyspace} WITH REPLICATION = {{ 'class' : 'NetworkTopologyStrategy', '{rf_opt}' : {new_rf} }}")
        def change_dc_rf(new_rf):
            change_opt_rf(this_dc, new_rf)
        def change_default_rf(new_rf):
            change_opt_rf("replication_factor", new_rf)

        change_dc_rf(2)
        change_dc_rf(3)

        with pytest.raises(InvalidRequest):
            change_dc_rf(5)
        with pytest.raises(InvalidRequest):
            change_dc_rf(1)
        with pytest.raises(InvalidRequest):
            change_dc_rf(10)

        # Changing the default replication factor doesn't affect any of the existing DCs,
        # so we can change it arbitrarily.
        change_default_rf(4)
        change_default_rf(2)
        change_default_rf(8)
        change_default_rf(1)
        change_default_rf(10)
