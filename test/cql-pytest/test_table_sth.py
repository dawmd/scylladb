
# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

###############################################################################
# Tests for server-side describe
###############################################################################

import pytest
from util import new_test_table

def test_table(cql, test_keyspace):
    with new_test_table(cql, test_keyspace, "p int PRIMARY KEY", "WITH crc_check_chance = 0.13") as table:
        _, table_name = table.split(".")
        
        result = cql.execute(f"SELECT crc_check_chance FROM system_schema.tables WHERE keyspace_name = '{test_keyspace}' AND table_name = '{table_name}'").one().crc_check_chance
        print(f"RESULT = {result}")
        assert result == 0.13
