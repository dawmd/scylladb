# Copyright 2024-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later

################################################################################
# ............................................................................ #
# ------------------------------- DESCRIPTION -------------------------------- #
# ............................................................................ #
# ============================================================================ #
#                                                                              #
# The tests below correspond to the task `scylladb/scylladb#18750`:            #
#     "auth on raft: safe backup and restore"                                  #
#                                                                              #
# We want to test the following features related to the issue:                 #
#                                                                              #
# 1. Creating roles when providing `SALTED HASH`,                              #
#                                                                              #
################################################################################

import pytest
from cassandra.protocol import BadCredentials, Unauthorized
from util import new_user, new_session

###

DEFAULT_SUPERUSER = "cassandra"

###

def sanitize_identifier(identifier: str, quotation_mark: str) -> str:
    doubled_quotation_mark = quotation_mark + quotation_mark
    return identifier.replace(quotation_mark, doubled_quotation_mark)

def sanitize_password(password: str) -> str:
    return sanitize_identifier(password, "'")

def make_identifier(identifier: str, quotation_mark: str) -> str:
    return quotation_mark + sanitize_identifier(identifier, quotation_mark) + quotation_mark

###

class AuthSLContext:
    def __init__(self, cql, ks=None):
        self.cql = cql
        self.ks = ks

    def __enter__(self):
        if self.ks:
            self.cql.execute(f"CREATE KEYSPACE {self.ks} WITH REPLICATION = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self.ks:
            self.cql.execute(f"DROP KEYSPACE {self.ks}")

        roles_iter = self.cql.execute(f"SELECT role FROM system.roles")
        roles_iter = filter(lambda record: record.role != DEFAULT_SUPERUSER, roles_iter)
        roles = [record.role for record in roles_iter]
        for role in roles:
            self.cql.execute(f"DROP ROLE {make_identifier(role, quotation_mark='"')}")

        service_levels_iter = self.cql.execute("LIST ALL SERVICE_LEVELS")
        service_levels = [record.service_level for record in service_levels_iter]
        for sl in service_levels:
            self.cql.execute(f"DROP SERVICE_LEVEL {make_identifier(sl, quotation_mark='"')}")

###

def test_create_role_with_salted_hash(cql):
    """
    Verify that creating a role with a salted hash works correctly, i.e. that the salted hash
    present in `system.roles` is the same as the one we provide.
    """

    with AuthSLContext(cql):
        role = "andrew"
        # Arbitrary salted hash. Could be anything.
        # We don't use characters that won't be generated, i.e.:
        #    `:`, `;`, `*`, `!`, and `\`,
        # but Scylla should technically accept them too.
        salted_hash = "@#$%^&()`,./{}[]abcdefghijklmnopqrstuwvxyzABCDEFGHIJKLMNOPQRSTUWVXYZ123456789~-_=+|"
        cql.execute(f"CREATE ROLE {role} WITH SALTED HASH = '{sanitize_password(salted_hash)}'")

        [result] = cql.execute(f"SELECT salted_hash FROM system.roles WHERE role = '{role}'")
        assert salted_hash == result.salted_hash

def test_create_role_with_invalid_salted_hash_and_try_to_login(cql):
    """
    Scylla doesn't verify that the provided salted hash is valid. For example, crypt(5) specifies
    that a salted hash will never contain a semicolon.
    We want to check that after creating a role with an invalid salted hash, attempting to log in
    with that role doesn't lead to any issues.
    """

    # For the time being, the code below doesn't work, so we comment it out.
    return

    with AuthSLContext(cql):
        role_cannot_login = "andrew"
        role_can_login = "jane"

        cql.execute(f"CREATE ROLE {role_cannot_login} WITH SALTED HASH = ';' AND LOGIN = false")
        cql.execute(f"CREATE ROLE {role_can_login} WITH SALTED HASH = ';' AND LOGIN = true")

        try:
            with new_session(cql, role_cannot_login):
                pytest.fail("Attempting to log in has not been rejected")
        except BadCredentials:
            pass


        try:
            with new_session(cql, role_can_login):
                pytest.fail("Attempting to log in has not been rejected")
        except BadCredentials:
            pass


@pytest.mark.skip
def test_create_role_with_salted_hash_autorization(cql):
    """
    Verify that roles that aren't superusers cannot perform `CREATE ROLE WITH SALTED HASH`.
    """

    with AuthSLContext(cql):
        def try_describe_with_passwords(role):
            try:
                with new_session(cql, role) as ncql:
                    ncql.execute("CREATE ROLE some_unused_name WITH SALTED HASH = 'somesaltedhash'")
                    pytest.fail(f"Executing the statement by {role} has not been rejected")
            except Unauthorized:
                pass

        # List of form (role name, list of permission grants to the role)
        r1 = "andrew"
        r2 = "jane"
        
        with new_user(cql, r1), new_user(cql, r2):
            # This also grants access to system tables.
            cql.execute(f"GRANT ALL ON ALL KEYSPACES TO {r2}")
            
            try_describe_with_passwords(r1)
            try_describe_with_passwords(r2)
        
        r3 = "bob"

        with new_user(cql, r3, with_superuser_privileges=True):
            with new_session(cql, r3) as ncql:
                ncql.execute("CREATE ROLE some_unusued_name WITH SALTED HASH = 'somesaltedhash'")


@pytest.mark.skip
def test_create_role_with_salted_hash_anonymous(cql):
    """
    Verify that `CREATE ROLE WITH SALTED HASH` cannot be performed by an anonymous user.
    """

    pass
