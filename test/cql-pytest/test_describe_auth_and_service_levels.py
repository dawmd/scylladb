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
# 2. The behavior of `DESC SCHEMA WITH INTERNALS (AND PASSWORDS)` and          #
#    the correctness of statements that it produces and which are related      #
#    to the referenced issue: auth and service levels.                         #
#                                                                              #
# To do that, we use the following pattern for most of the cases we should     #
# cover in this file:                                                          #
#                                                                              #
# 1. Format of the returned result by the query:                               #
#    - Are all of the values in the columns present and correct?               #
# 2. Formatting of identifiers with quotation marks.                           #
# 3. Formatting of identifiers with uppercase characters.                      #
# 4. Formatting of identifiers with unicode characters.                        #
# 5. Test(s) verifying that all of the cases are handled and `DESC SCHEMA`     #
#    prints them properly.                                                     #
#                                                                              #
################################################################################

from collections.abc import Iterable, Set
from typing import Any

import pytest
from cassandra.protocol import SyntaxException, Unauthorized
from util import new_user, new_session

################################################################################
# ............................................................................ #
# ---------------------------------- NOTES ----------------------------------- #
# ............................................................................ #
# ============================================================================ #
#                                                                              #
# 1. Every create statement corresponding to auth and service levels returned  #
#    by                                                                        #
#        `DESC SCHEMA WITH INTERNALS (AND PASSWORDS)`                          #
#    is termined with a semicolon.                                             #
#                                                                              #
# 2. `CREATE ROLE` statements always preserve the following order of options:  #
#        `SALTED HASH`, `LOGIN`, `SUPERUSER`                                   #
#    Aside from `SALTED HASH`, which only appears when executing               #
#        `DESC SCHEMA WITH INTERNALS AND PASSWORDS`                            #
#    the parameters are always present.                                        #
#                                                                              #
# 3. *ALL* create statements returned by                                       #
#        `DESC SCHEMA WITH INTERNALS (AND PASSWORDS)`                          #
#    and related to AUTH/service levels use capital letters for CQL syntax.    #
#                                                                              #
# 4. If an identifier needs to be quoted, e.g. because it contains whitespace  #
#    characters, it will be wrapped with double quoatation marks.              #
#    There are three exceptions to that rule:                                  #
#                                                                              #
#    (i)   the `WORKLOAD_TYPE` option when creating a service level,           #
#    (ii)  the `PASSWORD` option when creating a role,                         #
#    (iii) the `SALTED HASH` option when creating a role.                      #
#                                                                              #
#    The exceptions are enforced by the CQL grammar used in Scylla.            #
#                                                                              #
# 5. Statements for creating service levels always have options listed in the  #
#    following order:                                                          #
#        `TIMEOUT`, `WORKLOAD_TYPE`, `SHARES`                                  #
#    If an option is unnecessary (e.g. there's no timeout or the workload      #
#    type is unspecified -- default!), it's not present in the create          #
#    statement of the result of `DESC SCHEMA WITH INTERNALS`.                  #
#                                                                              #
# 6. The `TIMEOUT` option in `CREATE SERVICE_LEVEL` statements returned        #
#    by `DESC SCHEMA WITH INTERNALS` always uses milliseconds as its           #
#    resolution.                                                               #
#                                                                              #
# 7. We create test keyspaces manually here. The rationale for that is         #
#    the fact that the creator of a resource automatically obtains all         #
#    permissions on the resource. Since in these tests we verify permission    #
#    grants, we want to have full control over who creates what.               #
#                                                                              #
################################################################################

# Type of the row returned by `DESC` statements. It's of form
#   (keyspace_name, type, name, create_statement)
DescRowType = Any

DEFAULT_SUPERUSER = "cassandra"

def filter_non_default_user(desc_result_iter: Iterable[DescRowType]) -> Iterable[DescRowType]:
    return filter(lambda result: result.name != DEFAULT_SUPERUSER, desc_result_iter)

###

def filter_roles(desc_result_iter: Iterable[DescRowType]) -> Iterable[DescRowType]:
    return filter(lambda result: result.type == "role", desc_result_iter)

def filter_grant_roles(desc_result_iter: Iterable[DescRowType]) -> Iterable[DescRowType]:
    return filter(lambda result: result.type == "grant_role", desc_result_iter)

def filter_grant_permissions(desc_result_iter: Iterable[DescRowType]) -> Iterable[DescRowType]:
    return filter(lambda result: result.type == "grant_permission", desc_result_iter)

def filter_service_levels(desc_result_iter: Iterable[DescRowType]) -> Iterable[DescRowType]:
    return filter(lambda result: result.type == "service_level", desc_result_iter)

def filter_attached_service_levels(desc_result_iter: Iterable[DescRowType]) -> Iterable[DescRowType]:
    return filter(lambda result: result.type == "service_level_attachment", desc_result_iter)

###

def extract_names(desc_result_iter: Iterable[DescRowType]) -> Iterable[str]:
    return map(lambda result: result.name, desc_result_iter)

def extract_create_statements(desc_result_iter: Iterable[DescRowType]) -> Iterable[str]:
    return map(lambda result: result.create_statement, desc_result_iter)

###

def sanitize_identifier(identifier: str, quotation_mark: str) -> str:
    doubled_quotation_mark = quotation_mark + quotation_mark
    return identifier.replace(quotation_mark, doubled_quotation_mark)

def sanitize_password(password: str) -> str:
    return sanitize_identifier(password, "'")

def make_identifier(identifier: str, quotation_mark: str) -> str:
    return quotation_mark + sanitize_identifier(identifier, quotation_mark) + quotation_mark

###

KS_AND_TABLE_PERMISSIONS = ["CREATE", "ALTER", "DROP", "MODIFY", "SELECT", "AUTHORIZE"]

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

def test_create_role_with_salted_hash_syntax(cql):
    """
    Verify that Scylla rejects invalid syntax of `CREATE ROLE WITH SALTED HASH`.
    """

    # For clean-up if the test doesn't pass.
    with AuthSLContext(cql):
        with pytest.raises(SyntaxException):
            cql.execute("CREATE ROLE my_role WITH PASSWORD = 'hi' AND SALTED HASH = 'hello'")
        with pytest.raises(SyntaxException):
            cql.execute("CREATE ROLE my_role WITH SALTED HASH = 'hello' AND PASSWORD = 'hi'")
        with pytest.raises(SyntaxException):
            cql.execute("CREATE ROLE my_role WITH PASSWORD = 'hi' AND SALTED HASH = hello")
        with pytest.raises(SyntaxException):
            cql.execute("CREATE ROLE my_role WITH SALTED HASH = hello AND PASSWORD = 'hi'")
        with pytest.raises(SyntaxException):
            cql.execute("CREATE ROLE my_role WITH PASSWORD = hi AND SALTED HASH = 'hello'")
        with pytest.raises(SyntaxException):
            cql.execute("CREATE ROLE my_role WITH SALTED HASH = 'hello' AND PASSWORD = hi")
        with pytest.raises(SyntaxException):
            cql.execute("CREATE ROLE my_role WITH SALTED HASH = hello AND PASSWORD = hi")
        with pytest.raises(SyntaxException):
            cql.execute("CREATE ROLE my_role WITH SALTED HASH = hello AND PASSWORD = hi")
        with pytest.raises(SyntaxException):
            cql.execute("CREATE ROLE my_role WITH SALTED HASH = hello")
        with pytest.raises(SyntaxException):
            cql.execute("CREATE ROLE WITH SALTED HASH = 'hello'")
        with pytest.raises(SyntaxException):
            cql.execute("CREATE ROLE WITH SALTED HASH = hello")

def test_create_role_with_salted_hash_quotation_marks(cql):
    """
    Verify that Scylla processes quotation marks in `SALTED HASH` correctly.
    """

    with AuthSLContext(cql):
        cql.execute("CREATE ROLE my_role1 WITH SALTED HASH = 'my_hash''something'")
        cql.execute("CREATE ROLE my_role2 WITH SALTED HASH = '\"'")
        # Scylla's CQL requires that both `PASSWORD` and `SALTED HASH` are wrapped
        # in single quotation marks.
        with pytest.raises(SyntaxException):
            cql.execute('CREATE ROLE my_role3 WITH SALTED HASH = "my_hash"')

@pytest.mark.skip
def test_create_role_with_salted_hash_autorization(cql):
    """
    Verify that roles that aren't superusers cannot perform `CREATE ROLE WITH SALTED HASH`.
    """

    pass

@pytest.mark.skip
def test_create_role_with_salted_hash_anonymous(cql):
    """
    Verify that `CREATE ROLE WITH SALTED HASH` cannot be performed by an anonymous user.
    """

    pass

###

@pytest.mark.skip
def test_desc_autorization(cql):
    """
    Verify that Scylla rejects performing `DESC SCHEMA WITH INTERNALS AND PASSWORDS` if the user
    sending the request is not a superuser, even if they have all permissions to relevant system tables.
    """

    pass

@pytest.mark.skip
def test_desc_anonymous_role(cql):
    """
    Verify that `DESC SCHEMA WITH INTERNALS AND PASSWORDS` cannot be performed by an anonymous user.
    """

    pass

def test_desc_roles_format(cql):
    """
    Verify that the format of the output of `DESC SCHEMA WITH INTERNALS` corresponding to
    creating roles is of the expected form.
    """

    with AuthSLContext(cql):
        role_name = "andrew"
        stmt = f"CREATE ROLE {role_name} WITH LOGIN = false AND SUPERUSER = false;"
        cql.execute(stmt)

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_roles(desc_iter)
        desc_iter = filter_non_default_user(desc_iter)
        [result] = list(desc_iter)

        assert result.keyspace_name == '""'
        assert result.type == "role"
        assert result.name == role_name
        assert result.create_statement == stmt

def test_desc_roles_quotation_marks(cql):
    """
    Verify that statements corresponding to creating roles correctly format quotation marks.
    """

    with AuthSLContext(cql):
        andrew_raw = "andrew \" 'the great'"
        jane_raw = "jane ' \"the wise\""

        andrew_salted_hash_raw = "my \" 'salted hash'"
        jane_salted_hash_raw = "my ' \"other salted hash\""

        andrew_single_quote = make_identifier(andrew_raw, quotation_mark="'")
        andrew_double_quote = make_identifier(andrew_raw, quotation_mark='"')
        jane_double_quote = make_identifier(jane_raw, quotation_mark='"')

        andrew_salted_hash = make_identifier(andrew_salted_hash_raw, quotation_mark="'")
        jane_salted_hash = make_identifier(jane_salted_hash_raw, quotation_mark="'")

        cql.execute(f"CREATE ROLE {andrew_single_quote} WITH SALTED HASH = {andrew_salted_hash}")
        cql.execute(f"CREATE ROLE {jane_double_quote} WITH SALTED HASH = {jane_salted_hash}")

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS AND PASSWORDS")
        desc_iter = filter_roles(desc_iter)
        desc_iter = filter_non_default_user(desc_iter)

        desc_elements = list(desc_iter)
        element_names_iter = extract_names(desc_elements)

        assert set(element_names_iter) == {andrew_double_quote, jane_double_quote}

        desc_iter = extract_create_statements(desc_elements)

        expected_result = {
            f"CREATE ROLE {andrew_double_quote} WITH SALTED HASH = {andrew_salted_hash} AND LOGIN = false AND SUPERUSER = false;",
            f"CREATE ROLE {jane_double_quote} WITH SALTED HASH = {jane_salted_hash} AND LOGIN = false AND SUPERUSER = false;"
        }

        assert set(desc_iter) == expected_result

def test_desc_roles_uppercase(cql):
    """
    Verify that statements corresponding to creating roles correctly format uppercase characters,
    i.e. identifiers like that should be wrapped in quotation marks.
    """

    with AuthSLContext(cql):
        role = '"myRole"'
        cql.execute(f"CREATE ROLE {role}")

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_roles(desc_iter)
        desc_iter = filter_non_default_user(desc_iter)

        desc_elements = list(desc_iter)

        role_iter = map(lambda row: row.name, desc_elements)
        assert list(role_iter) == [role]

        desc_iter = extract_create_statements(desc_elements)

        assert list(desc_iter) == [f"CREATE ROLE {role} WITH LOGIN = false AND SUPERUSER = false;"]

def test_desc_roles_unicode(cql):
    """
    Verify that statements to creating roles can contain unicode characters.
    """

    with AuthSLContext(cql):
        role = '"ユーザー"'
        cql.execute(f"CREATE ROLE {role}")

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_roles(desc_iter)
        desc_iter = filter_non_default_user(desc_iter)

        desc_elements = list(desc_iter)

        role_iter = map(lambda row: row.name, desc_elements)
        assert list(role_iter) == [role]

        desc_iter = extract_create_statements(desc_elements)

        assert list(desc_iter) == [f"CREATE ROLE {role} WITH LOGIN = false AND SUPERUSER = false;"]

def test_desc_roles(cql):
    """
    Verify that the output of `DESC SCHEMA WITH INTERNALS` corresponding to creating roles
    is as expected for various different cases.
    """

    with AuthSLContext(cql):
        roles = ["andrew", "alice", '"m!ch@el j4cks0n"']
        roles_password = ["fred", "julie", '"hi, I like sunsets"']
        roles_can_login = ["bob", "jane", '"very weird nam3 Fu!! of character$"']
        roles_superuser = ["gustang", "devon", '"my h4ppy c0mp@n!0n!"']
        roles_can_login_and_superuser = ["peter", "susan", '"the k!ng 0f 3vryth!ng th4t 3x!$t$ @"']

        for idx, role in enumerate(roles):
            cql.execute(f"CREATE ROLE {role}")
        for idx, role in enumerate(roles_password):
            cql.execute(f"CREATE ROLE {role} WITH PASSWORD = 'my_password{idx}'")
        for role in roles_can_login:
            cql.execute(f"CREATE ROLE {role} WITH LOGIN = true")
        for role in roles_superuser:
            cql.execute(f"CREATE ROLE {role} WITH SUPERUSER = true")
        for role in roles_can_login_and_superuser:
            cql.execute(f"CREATE ROLE {role} WITH SUPERUSER = true AND LOGIN = true")

        create_role_stmts = [
            [f"CREATE ROLE {role} WITH LOGIN = false AND SUPERUSER = false;" for role in roles],
            [f"CREATE ROLE {role} WITH LOGIN = false AND SUPERUSER = false;" for role in roles_password],
            [f"CREATE ROLE {role} WITH LOGIN = true AND SUPERUSER = false;" for role in roles_can_login],
            [f"CREATE ROLE {role} WITH LOGIN = false AND SUPERUSER = true;" for role in roles_superuser],
            [f"CREATE ROLE {role} WITH LOGIN = true AND SUPERUSER = true;" for role in roles_can_login_and_superuser],
            [f"CREATE ROLE IF NOT EXISTS {DEFAULT_SUPERUSER} WITH LOGIN = true AND SUPERUSER = true;"]
        ]
        # Flatten the list of lists to a list.
        create_role_stmts = sum(create_role_stmts, [])
        create_role_stmts = set(create_role_stmts)

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_roles(desc_iter)
        desc_create_role_stmts = set(extract_create_statements(desc_iter))

        assert create_role_stmts == desc_create_role_stmts

def test_desc_roles_with_passwords(cql):
    """
    Verify that the output of `DESC SCHEMA WITH INTERNALS AND PASSWORDS` corresponding to creating roles
    is as expected for various different cases.
    """

    with AuthSLContext(cql):
        role_without_pass = "bob"
        role_with_pass = "alice"

        create_stmt_without_pass = f"CREATE ROLE {role_without_pass} WITH LOGIN = false AND SUPERUSER = false;"
        create_stmt_with_pass = f"CREATE ROLE {role_with_pass} WITH PASSWORD = 'some_funky_password'"

        cql.execute(create_stmt_without_pass)
        cql.execute(create_stmt_with_pass)

        [salted_hash_result] = cql.execute(f"SELECT salted_hash FROM system.roles WHERE role = '{role_with_pass}'")
        salted_hash = salted_hash_result.salted_hash
        create_stmt_with_salted_hash = f"CREATE ROLE {role_with_pass} WITH SALTED HASH = '{sanitize_password(salted_hash)}' AND LOGIN = false AND SUPERUSER = false;"

        stmts = [create_stmt_without_pass, create_stmt_with_salted_hash]

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS AND PASSWORDS")
        desc_iter = filter_roles(desc_iter)
        desc_iter = filter_non_default_user(desc_iter)
        desc_iter = extract_create_statements(desc_iter)

        assert set(stmts) == set(desc_iter)

def test_desc_role_grants_format(cql):
    """
    Verify that the format of the output of `DESC SCHEMA WITH INTERNALS` corresponding to
    granting roles is of the expected form.
    """

    with AuthSLContext(cql):
        [r1, r2] = ["andrew", "jane"]

        cql.execute(f"CREATE ROLE {r1}")
        cql.execute(f"CREATE ROLE {r2}")

        stmt = f"GRANT {r1} TO {r2};"
        cql.execute(stmt)

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_grant_roles(desc_iter)

        [result] = list(desc_iter)

        assert result.keyspace_name == '""'
        assert result.type == "grant_role"
        assert result.name == r1
        assert result.create_statement == stmt

def test_desc_role_grants_quotation_marks(cql):
    """
    Verify that statements corresponding to granting roles correctly format quotation marks.
    """

    with AuthSLContext(cql):
        andrew_raw = "andrew \" 'the great'"
        jane_raw = "jane ' \"the wise\""

        andrew_single_quote = make_identifier(andrew_raw, quotation_mark="'")
        andrew_double_quote = make_identifier(andrew_raw, quotation_mark='"')
        jane_double_quote = make_identifier(jane_raw, quotation_mark='"')

        cql.execute(f"CREATE ROLE {andrew_single_quote}")
        cql.execute(f"CREATE ROLE {jane_double_quote}")

        cql.execute(f"GRANT {andrew_single_quote} TO {jane_double_quote}")

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_grant_roles(desc_iter)

        desc_elements = [*desc_iter]
        element_names_iter = extract_names(desc_elements)

        assert set(element_names_iter) == {andrew_double_quote}

        desc_iter = extract_create_statements(desc_elements)

        expected_result = f"GRANT {andrew_double_quote} TO {jane_double_quote};"
        assert [expected_result] == list(desc_iter)

def test_desc_role_grants_uppercase(cql):
    """
    Verify that statements corresponding to granting roles correctly format uppercase characters,
    i.e. identifiers like that should be wrapped in quotation marks.
    """

    with AuthSLContext(cql):
        r1 = '"myRole"'
        r2 = '"otherRole"'

        cql.execute(f"CREATE ROLE {r1}")
        cql.execute(f"CREATE ROLE {r2}")
        cql.execute(f"GRANT {r1} TO {r2}")

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_grant_roles(desc_iter)

        desc_elements = list(desc_iter)

        role_iter = map(lambda row: row.name, desc_elements)
        assert list(role_iter) == [r1]

        desc_iter = extract_create_statements(desc_elements)

        assert list(desc_iter) == [f"GRANT {r1} TO {r2};"]

def test_desc_role_grants_unicode(cql):
    """
    Verify that statements corresponding to granting roles correctly format unicode characters,
    i.e. identifiers like that should be wrapped in quotation marks.
    """

    with AuthSLContext(cql):
        r1 = '"ユーザー"'
        r2 = '"私の猫"'

        cql.execute(f"CREATE ROLE {r1}")
        cql.execute(f"CREATE ROLE {r2}")
        cql.execute(f"GRANT {r1} TO {r2}")

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_grant_roles(desc_iter)

        desc_elements = list(desc_iter)

        role_iter = map(lambda row: row.name, desc_elements)
        assert list(role_iter) == [r1]

        desc_iter = extract_create_statements(desc_elements)

        assert list(desc_iter) == [f"GRANT {r1} TO {r2};"]

def test_desc_role_grants(cql):
    """
    Verify that the output of `DESC SCHEMA WITH INTERNALS` corresponding to granting roles
    is as expected for various different cases.
    """

    with AuthSLContext(cql):
        [r1, r2, r3, r4] = ["andrew", '"b0b, my f@vor!t3 fr!3nd :)"', "jessica", "kate"]
        # List whose each element is a pair of form:
        #     (role, list of granted roles)
        roles = [
            (r1, []),
            (r2, [r1]),
            (r3, [r2]),
            (r4, [r1, r3])
        ]

        for role, _ in roles:
            cql.execute(f"CREATE ROLE {role}")
        for role, grants in roles:
            for grant in grants:
                cql.execute(f"GRANT {grant} TO {role}")

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_grant_roles(desc_iter)
        desc_grants = list(extract_create_statements(desc_iter))

        expected_grants = [[f"GRANT {grant} TO {role};" for grant in grants] for role, grants in roles]
        # Flatten the list of lists to a list.
        expected_grants = sum(expected_grants, [])

        assert set(expected_grants) == set(desc_grants)

def test_desc_grant_permission_format(cql):
    """
    Verify that the format of the output of `DESC SCHEMA WITH INTERNALS` corresponding to
    granting permissions is of the expected form.
    """

    with AuthSLContext(cql):
        role_name = "kate"
        cql.execute(f"CREATE ROLE {role_name}")

        stmt = f"GRANT SELECT ON ALL KEYSPACES TO {role_name};"
        cql.execute(stmt)

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_grant_permissions(desc_iter)
        # We need to fliter out the default superuser because when it creates a role,
        # it automatically obtains all of permissions to manipulate it.
        desc_iter = filter_non_default_user(desc_iter)

        [result] = list(desc_iter)

        assert result.keyspace_name == '""'
        assert result.type == "grant_permission"
        assert result.name == role_name
        assert result.create_statement == stmt

def test_desc_grant_permission_quotation_marks(cql):
    """
    Verify that statements corresponding to granting permissions correctly format quotation marks.
    """

    with AuthSLContext(cql):
        andrew_raw = "andrew \" 'the great'"
        jane_raw = "jane ' \"the wise\""

        andrew_single_quote = make_identifier(andrew_raw, quotation_mark="'")
        andrew_double_quote = make_identifier(andrew_raw, quotation_mark='"')
        jane_double_quote = make_identifier(jane_raw, quotation_mark='"')

        cql.execute(f"CREATE ROLE {andrew_single_quote}")
        cql.execute(f"CREATE ROLE {jane_double_quote}")

        cql.execute(f"GRANT SELECT ON ALL KEYSPACES TO {andrew_single_quote}")
        cql.execute(f"GRANT ALTER ON ALL KEYSPACES TO {jane_double_quote}")

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_grant_permissions(desc_iter)
        desc_iter = filter_non_default_user(desc_iter)

        desc_elements = list(desc_iter)
        element_names_iter = extract_names(desc_elements)

        assert set(element_names_iter) == {andrew_double_quote, jane_double_quote}

        desc_iter = extract_create_statements(desc_elements)

        expected_result = {
            f"GRANT SELECT ON ALL KEYSPACES TO {andrew_double_quote};",
            f"GRANT ALTER ON ALL KEYSPACES TO {jane_double_quote};"
        }

        assert set(desc_iter) == expected_result

def test_desc_auth_different_permissions(cql):
    """
    Verify that the output of `DESC SCHEMA WITH INTERNALS` corresponding to granting permissions
    is as expected for various different cases. Here we test different kinds of permissions specifically.
    """

    with AuthSLContext(cql, ks="my_ks") as ctx:
        all_ks_grants = [f"GRANT {permission} ON ALL KEYSPACES TO {{}};" for permission in KS_AND_TABLE_PERMISSIONS]
        specific_ks_grants = [f"GRANT {permission} ON KEYSPACE {ctx.ks} TO {{}};" for permission in KS_AND_TABLE_PERMISSIONS]

        grants = [*all_ks_grants, *specific_ks_grants, r"GRANT DESCRIBE ON ALL ROLES TO {};"]

        roles = [f"my_role_{idx}" for idx in range(len(grants))]
        grants = [grant.format(roles[idx]) for idx, grant in enumerate(grants)]

        for role in roles:
            cql.execute(f"CREATE ROLE {role}")

        for grant in grants:
            cql.execute(grant)

        # Creating a resource automatically grants all permissioners on it to the creator.
        # That's why we need to prepare that the describe statement below will return
        # additional grants for the default superuser.
        schema_superuser_perms = [f"GRANT {permission} ON KEYSPACE {ctx.ks} TO {DEFAULT_SUPERUSER};"
                                  for permission in KS_AND_TABLE_PERMISSIONS]
        role_superuser_perms = [f"GRANT {permission} ON ROLE {role} TO {DEFAULT_SUPERUSER};"
                                for role in roles for permission in ["AUTHORIZE", "ALTER", "DROP"]]
        stmts = [*grants, *schema_superuser_perms, *role_superuser_perms]

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_grant_permissions(desc_iter)
        desc_iter = extract_create_statements(desc_iter)

        assert set(stmts) == set(desc_iter)

def test_desc_data_permissions_uppercase(cql):
    """
    Verify that statements corresponding to granting data permissions correctly format uppercase characters,
    i.e. identifiers like that should be wrapped in quotation marks.
    """

    ks = '"myKs"'
    with AuthSLContext(cql, ks=ks):
        r1 = '"myRole"'
        r2 = '"someOtherRole"'
        r3 = '"YetANOTHERrole"'

        roles = {r1, r2, r3}

        for role in roles:
            cql.execute(f"CREATE ROLE {role}")

        table = '"myTable"'
        cql.execute(f"CREATE TABLE {ks}.{table} (pk int PRIMARY KEY, t int)")

        stmts = {
            f"GRANT SELECT ON ALL KEYSPACES TO {r1};",
            f"GRANT SELECT ON KEYSPACE {ks} TO {r2};",
            f"GRANT SELECT ON {ks}.{table} TO {r3};"
        }

        for stmt in stmts:
            cql.execute(stmt)

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_grant_permissions(desc_iter)
        desc_iter = filter_non_default_user(desc_iter)
        desc_elements = list(desc_iter)

        roles_iter = map(lambda row: row.name, desc_elements)
        assert set(roles_iter) == roles

        desc_iter = extract_create_statements(desc_elements)
        assert set(desc_iter) == stmts

def test_desc_data_permissions_unicode(cql):
    """
    Verify that statements corresponding to granting permissions to data resources correctly format
    unicode characters, i.e. identifiers like that should be wrapped in quotation marks.
    """

    with AuthSLContext(cql, ks="my_ks") as ctx:
        r1 = '"ユーザー"'
        r2 = '"私の猫"'
        r3 = '"山羊"'

        roles = {r1, r2, r3}

        for role in roles:
            cql.execute(f"CREATE ROLE {role}")

        table = "my_table"
        cql.execute(f"CREATE TABLE {ctx.ks}.{table} (pk int PRIMARY KEY, t int)")

        stmts = {
            f"GRANT SELECT ON ALL KEYSPACES TO {r1};",
            f"GRANT SELECT ON KEYSPACE {ctx.ks} TO {r2};",
            f"GRANT SELECT ON {ctx.ks}.{table} TO {r3};"
        }

        for stmt in stmts:
            cql.execute(stmt)

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_grant_permissions(desc_iter)
        desc_iter = filter_non_default_user(desc_iter)
        desc_elements = list(desc_iter)

        roles_iter = map(lambda row: row.name, desc_elements)
        assert set(roles_iter) == roles

        desc_iter = extract_create_statements(desc_elements)
        assert set(desc_iter) == stmts

def test_desc_data_permissions(cql):
    """
    Verify that the output of `DESC SCHEMA WITH INTERNALS` corresponding to granting permissions
    is as expected for various different cases. Here we test data resources specifically.
    """

    with AuthSLContext(cql, ks="my_ks") as ctx:
        all_ks_role = "mary"
        spec_ks_role = '"r0bb, my gr3@t3st fr!3nd :)"'
        spec_table_role = "scarlet"

        for role in [all_ks_role, spec_ks_role, spec_table_role]:
            cql.execute(f"CREATE ROLE {role}")

        table_name = "my_table"
        # Note: When the keyspace `ctx.ks` is dropped, the table will be removed as well.
        #       That's why there's no need to clean up this table later.
        cql.execute(f"CREATE TABLE {ctx.ks}.{table_name} (a int PRIMARY KEY, b int)")

        all_ks_stmt = f"GRANT CREATE ON ALL KEYSPACES TO {all_ks_role};"
        spec_ks_stmt = f"GRANT ALTER ON KEYSPACE {ctx.ks} TO {spec_ks_role};"
        spec_table_stmt = f"GRANT MODIFY ON {ctx.ks}.{table_name} TO {spec_table_role};"

        stmts = [all_ks_stmt, spec_ks_stmt, spec_table_stmt]
        for stmt in stmts:
            cql.execute(stmt)

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_grant_permissions(desc_iter)
        desc_iter = filter_non_default_user(desc_iter)
        desc_iter = extract_create_statements(desc_iter)

        assert set(stmts) == set(desc_iter)

def test_desc_role_permissions_uppercase(cql):
    """
    Verify that statements corresponding to granting role permissions correctly format uppercase characters,
    i.e. identifiers like that should be wrapped in quotation marks.
    """

    with AuthSLContext(cql):
        r1 = '"myRole"'
        r2 = '"MyOtherRole"'

        roles = {r1, r2}
        for role in roles:
            cql.execute(f"CREATE ROLE {role}")

        stmts = {
            f"GRANT AUTHORIZE ON ALL ROLES TO {r1};",
            f"GRANT ALTER ON ROLE {r1} TO {r2};"
        }

        for stmt in stmts:
            cql.execute(stmt)

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_grant_permissions(desc_iter)
        desc_iter = filter_non_default_user(desc_iter)

        desc_elements = list(desc_iter)

        role_iter = map(lambda row: row.name, desc_elements)
        assert set(role_iter) == roles

        desc_iter = extract_create_statements(desc_elements)
        assert set(desc_iter) == stmts

def test_desc_role_permissions_unicode(cql):
    """
    Verify that statements corresponding to granting permissions to role resources correctly format
    unicode characters, i.e. identifiers like that should be wrapped in quotation marks.
    """

    with AuthSLContext(cql):
        r1 = '"ユーザー"'
        r2 = '"私の猫"'

        roles = {r1, r2}
        for role in roles:
            cql.execute(f"CREATE ROLE {role}")

        stmts = {
            f"GRANT AUTHORIZE ON ALL ROLES TO {r1};",
            f"GRANT ALTER ON ROLE {r1} TO {r2};"
        }

        for stmt in stmts:
            cql.execute(stmt)

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_grant_permissions(desc_iter)
        desc_iter = filter_non_default_user(desc_iter)

        desc_elements = list(desc_iter)

        role_iter = map(lambda row: row.name, desc_elements)
        assert set(role_iter) == roles

        desc_iter = extract_create_statements(desc_elements)
        assert set(desc_iter) == stmts

def test_desc_role_permissions(cql):
    """
    Verify that the output of `DESC SCHEMA WITH INTERNALS` corresponding to granting permissions
    is as expected for various different cases. Here we test role permissions specifically.
    """

    with AuthSLContext(cql):
        all_roles_role = "howard"
        specific_role_role = '"h! th3r3 str@ng3r :)"'

        for role in [all_roles_role, specific_role_role]:
            cql.execute(f"CREATE ROLE {role}")

        all_roles_stmt = f"GRANT AUTHORIZE ON ALL ROLES TO {all_roles_role};"
        specific_role_stmt = f"GRANT ALTER ON ROLE {all_roles_role} TO {specific_role_role};"

        stmts = [all_roles_stmt, specific_role_stmt]
        for stmt in stmts:
            cql.execute(stmt)

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_grant_permissions(desc_iter)
        desc_iter = filter_non_default_user(desc_iter)
        desc_iter = extract_create_statements(desc_iter)

        assert set(stmts) == set(desc_iter)

def test_desc_udf_permissions_uppercase(cql):
    """
    Verify that statements corresponding to granting permissions to UDFs correctly format uppercase characters,
    i.e. identifiers like that should be wrapped in quotation marks.
    """

    ks = '"myKs"'
    with AuthSLContext(cql, ks=ks):
        all_funcs_role = '"myRole"'
        all_funcs_in_ks_role = '"MyOtherRole"'
        specific_func_role = '"ROLE"'

        for role in [all_funcs_role, all_funcs_in_ks_role, specific_func_role]:
            cql.execute(f"CREATE ROLE {role}")

        func_name = '"functionName"'
        type_name = '"TypeName"'

        cql.execute(f"CREATE TYPE {ks}.{type_name} (value int)")
        cql.execute(f"""CREATE FUNCTION {ks}.{func_name}(val1 int, val2 {type_name})
                        RETURNS NULL ON NULL INPUT
                        RETURNS int
                        LANGUAGE lua
                        AS $$ return val1 + val2.value $$""")

        stmts = {
            f"GRANT ALTER ON ALL FUNCTIONS TO {all_funcs_role};",
            f"GRANT EXECUTE ON ALL FUNCTIONS IN KEYSPACE {ks} TO {all_funcs_in_ks_role};",
            f"GRANT DROP ON FUNCTION {ks}.{func_name}(int, {type_name}) TO {specific_func_role};"
        }

        for stmt in stmts:
            cql.execute(stmt)

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_grant_permissions(desc_iter)
        desc_iter = filter_non_default_user(desc_iter)

        desc_elements = list(desc_iter)

        role_iter = map(lambda row: row.name, desc_elements)
        assert set(role_iter) == {all_funcs_role, all_funcs_in_ks_role, specific_func_role}

        desc_iter = extract_create_statements(desc_elements)
        assert set(desc_iter) == stmts

def test_desc_udf_permissions_unicode(cql):
    """
    Verify that statements corresponding to granting permissions to UDFs correctly format
    unicode characters, i.e. identifiers like that should be wrapped in quotation marks.
    """

    with AuthSLContext(cql, ks="my_ks") as ctx:
        all_funcs_role = '"ユーザー"'
        all_funcs_in_ks_role = '"私の猫"'
        specific_func_role = '"山羊"'

        for role in [all_funcs_role, all_funcs_in_ks_role, specific_func_role]:
            cql.execute(f"CREATE ROLE {role}")

        func_name = '"関数"'
        type_name = '"データ型"'

        cql.execute(f"CREATE TYPE {ctx.ks}.{type_name} (value int)")
        cql.execute(f"""CREATE FUNCTION {ctx.ks}.{func_name}(val1 int, val2 {type_name})
                        RETURNS NULL ON NULL INPUT
                        RETURNS int
                        LANGUAGE lua
                        AS $$ return val1 + val2.value $$""")

        stmts = {
            f"GRANT ALTER ON ALL FUNCTIONS TO {all_funcs_role};",
            f"GRANT EXECUTE ON ALL FUNCTIONS IN KEYSPACE {ctx.ks} TO {all_funcs_in_ks_role};",
            f"GRANT DROP ON FUNCTION {ctx.ks}.{func_name}(int, {type_name}) TO {specific_func_role};"
        }

        for stmt in stmts:
            cql.execute(stmt)

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_grant_permissions(desc_iter)
        desc_iter = filter_non_default_user(desc_iter)

        desc_elements = list(desc_iter)

        role_iter = map(lambda row: row.name, desc_elements)
        assert set(role_iter) == {all_funcs_role, all_funcs_in_ks_role, specific_func_role}

        desc_iter = extract_create_statements(desc_elements)
        assert set(desc_iter) == stmts

def test_desc_udf_permissions(cql):
    """
    Verify that the output of `DESC SCHEMA WITH INTERNALS` corresponding to granting permissions
    is as expected for various different cases. Here we test UDFs specifically.
    """

    with AuthSLContext(cql, ks="my_ks") as ctx:
        all_funcs_role = "miley"
        all_funcs_in_ks_role = '"v3ry w3!rd n@m3!"'
        specific_func_role = "ricard"

        for role in [all_funcs_role, all_funcs_in_ks_role, specific_func_role]:
            cql.execute(f"CREATE ROLE {role}")

        func_name = '"funct!0n n@m3"'
        type_name = '"qu!t3 int3r3st!ng typ3 :)"'

        cql.execute(f"CREATE TYPE {ctx.ks}.{type_name} (value int)")
        cql.execute(f"""CREATE FUNCTION {ctx.ks}.{func_name}(val1 int, val2 {type_name})
                        RETURNS NULL ON NULL INPUT
                        RETURNS int
                        LANGUAGE lua
                        AS $$ return val1 + val2.value $$""")

        stmts = {
            f"GRANT ALTER ON ALL FUNCTIONS TO {all_funcs_role};",
            f"GRANT EXECUTE ON ALL FUNCTIONS IN KEYSPACE {ctx.ks} TO {all_funcs_in_ks_role};",
            f"GRANT DROP ON FUNCTION {ctx.ks}.{func_name}(int, {type_name}) TO {specific_func_role};"
        }

        for stmt in stmts:
            cql.execute(stmt)

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_grant_permissions(desc_iter)
        desc_iter = filter_non_default_user(desc_iter)

        desc_elements = list(desc_iter)

        role_iter = map(lambda row: row.name, desc_elements)
        assert set(role_iter) == {all_funcs_role, all_funcs_in_ks_role, specific_func_role}

        desc_iter = extract_create_statements(desc_elements)
        assert set(desc_iter) == stmts

def test_desc_service_levels_format(cql):
    """
    Verify that the format of the output of `DESC SCHEMA WITH INTERNALS` corresponding to
    creating service levels is of the expected form.
    """

    with AuthSLContext(cql):
        sl_name = "my_service_level"
        stmt = f"CREATE SERVICE_LEVEL {sl_name};"
        cql.execute(stmt)

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_service_levels(desc_iter)

        [result] = list(desc_iter)

        assert result.keyspace_name == '""'
        assert result.type == "service_level"
        assert result.name == sl_name
        assert result.create_statement == stmt

def test_desc_service_levels_quotation_marks(cql):
    """
    Verify that statements corresponding to creating service levels correctly format quotation marks.
    """

    with AuthSLContext(cql):
        sl1_raw = "service \" 'level maybe'"
        sl2_raw = "service ' \"level perhaps\""

        sl1_single_quote = make_identifier(sl1_raw, quotation_mark="'")
        sl1_double_quote = make_identifier(sl1_raw, quotation_mark='"')
        sl2_double_quote = make_identifier(sl2_raw, quotation_mark='"')

        cql.execute(f"CREATE SERVICE_LEVEL {sl1_single_quote}")
        cql.execute(f"CREATE SERVICE_LEVEL {sl2_double_quote}")

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_service_levels(desc_iter)

        desc_elements = list(desc_iter)
        element_names_iter = extract_names(desc_elements)

        assert set(element_names_iter) == {sl1_double_quote, sl2_double_quote}

        desc_iter = extract_create_statements(desc_elements)

        expected_result = {
            f"CREATE SERVICE_LEVEL {sl1_double_quote};",
            f"CREATE SERVICE_LEVEL {sl2_double_quote};"
        }

        assert set(desc_iter) == expected_result

def test_desc_service_levels_uppercase(cql):
    """
    Verify that statements corresponding to creating service levels correctly format uppercase characters,
    i.e. identifiers like that should be wrapped in quotation marks.
    """

    with AuthSLContext(cql):
        sl = '"myServiceLevel"'
        cql.execute(f"CREATE SERVICE_LEVEL {sl}")

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_service_levels(desc_iter)

        desc_elements = list(desc_iter)

        sl_iter = map(lambda row: row.name, desc_elements)
        assert list(sl_iter) == [sl]

        desc_iter = extract_create_statements(desc_elements)
        assert list(desc_iter) == [f"CREATE SERVICE_LEVEL {sl};"]

def test_desc_service_levels_unicode(cql):
    """
    Verify that statements corresponding to creating service levels correctly format
    unicode characters, i.e. identifiers like that should be wrapped in quotation marks.
    """

    with AuthSLContext(cql):
        sl = '"レベル"'
        cql.execute(f"CREATE SERVICE_LEVEL {sl}")

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_service_levels(desc_iter)

        desc_elements = list(desc_iter)

        sl_iter = map(lambda row: row.name, desc_elements)
        assert list(sl_iter) == [sl]

        desc_iter = extract_create_statements(desc_elements)
        assert list(desc_iter) == [f"CREATE SERVICE_LEVEL {sl};"]

def test_desc_auth_service_levels(cql):
    """
    Verify that the output of `DESC SCHEMA WITH INTERNALS` corresponding to creating service levels
    is as expected for various different cases.
    """

    with AuthSLContext(cql):
        create_sl = ["CREATE SERVICE_LEVEL {};"]
        # Note: `CREATE SERVICE_LEVEL` statements returned by `DESC SCHEMA WITH INTERNALS` always uses
        #       `std::chrono::milliseconds` as its resolution. For that reason, we use milliseconds in
        #       these statements too to reuse them later in the assert.
        create_sl_time = [f"CREATE SERVICE_LEVEL {{}} WITH TIMEOUT = {timeout};" for timeout in ["10ms", "350ms", "20000ms"]]
        create_sl_wl_type = [f"CREATE SERVICE_LEVEL {{}} WITH WORKLOAD_TYPE = '{work_type}';"
                            for work_type in ["interactive", "batch"]]
        create_sl_time_and_wl_type = [f"CREATE SERVICE_LEVEL {{}} WITH TIMEOUT = {timeout} AND WORKLOAD_TYPE = '{work_type}';"
                                    for work_type in ["interactive", "batch"] for timeout in ["10ms", "350ms", "20000ms"]]

        create_sl_stmts = [*create_sl, *create_sl_time, *create_sl_wl_type, *create_sl_time_and_wl_type]
        create_sl_stmts = [stmt.format(f'"my f@v0rit3 s3rv!c3 l3v3l!! !nd3x {idx}"') for idx, stmt in enumerate(create_sl_stmts)]

        for stmt in create_sl_stmts:
            cql.execute(stmt)

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_service_levels(desc_iter)
        desc_iter = extract_create_statements(desc_iter)

        assert set(create_sl_stmts) == set(desc_iter)

def test_desc_attach_service_level_format(cql):
    """
    Verify that the format of the output of `DESC SCHEMA WITH INTERNALS` corresponding to
    attaching service levels is of the expected form.
    """

    with AuthSLContext(cql):
        role_name = "jasmine"
        cql.execute(f"CREATE ROLE {role_name}")

        sl_name = "some_service_level"
        cql.execute(f"CREATE SERVICE_LEVEL {sl_name}")

        stmt = f"ATTACH SERVICE_LEVEL {sl_name} TO {role_name};"
        cql.execute(stmt)

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_attached_service_levels(desc_iter)

        [result] = list(desc_iter)

        assert result.keyspace_name == '""'
        assert result.type == "service_level_attachment"
        assert result.name == sl_name
        assert result.create_statement == stmt

def test_desc_auth_attach_service_levels_quotation_marks(cql):
    """
    Verify that statements corresponding to attaching service levels correctly format quotation marks.
    """

    with AuthSLContext(cql):
        andrew_raw = "andrew \" 'the great'"
        jane_raw = "jane ' \"the wise\""

        andrew_single_quote = make_identifier(andrew_raw, quotation_mark="'")
        andrew_double_quote = make_identifier(andrew_raw, quotation_mark='"')
        jane_double_quote = make_identifier(jane_raw, quotation_mark='"')

        cql.execute(f"CREATE ROLE {andrew_single_quote}")
        cql.execute(f"CREATE ROLE {jane_double_quote}")

        sl1_raw = "service \" 'level maybe'"
        sl2_raw = "service ' \"level perhaps\""

        sl1_single_quote = make_identifier(sl1_raw, quotation_mark="'")
        sl1_double_quote = make_identifier(sl1_raw, quotation_mark='"')
        sl2_double_quote = make_identifier(sl2_raw, quotation_mark='"')

        cql.execute(f"CREATE SERVICE_LEVEL {sl1_single_quote}")
        cql.execute(f"CREATE SERVICE_LEVEL {sl2_double_quote}")

        cql.execute(f"ATTACH SERVICE_LEVEL {sl1_single_quote} TO {andrew_single_quote}")
        cql.execute(f"ATTACH SERVICE_LEVEL {sl2_double_quote} TO {jane_double_quote}")

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_attached_service_levels(desc_iter)

        desc_elements = list(desc_iter)
        element_names_iter = extract_names(desc_elements)

        assert set(element_names_iter) == {sl1_double_quote, sl2_double_quote}

        desc_iter = extract_create_statements(desc_elements)

        expected_result = {
            f"ATTACH SERVICE_LEVEL {sl1_double_quote} TO {andrew_double_quote};",
            f"ATTACH SERVICE_LEVEL {sl2_double_quote} TO {jane_double_quote};"
        }

        assert set(desc_iter) == expected_result

def test_desc_auth_attach_service_levels_uppercase(cql):
    """
    Verify that statements corresponding to attaching service levels correctly format uppercase characters,
    i.e. identifiers like that should be wrapped in quotation marks.
    """

    with AuthSLContext(cql):
        role = '"myRole"'
        sl = '"MyServiceLevel"'

        cql.execute(f"CREATE ROLE {role}")
        cql.execute(f"CREATE SERVICE_LEVEL {sl}")
        cql.execute(f"ATTACH SERVICE_LEVEL {sl} TO {role}")

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_attached_service_levels(desc_iter)

        desc_elements = list(desc_iter)

        sl_iter = map(lambda row: row.name, desc_elements)
        assert list(sl_iter) == [sl]

        desc_iter = extract_create_statements(desc_elements)
        assert list(desc_iter) == [f"ATTACH SERVICE_LEVEL {sl} TO {role};"]

def test_desc_attach_service_levels_unicode(cql):
    """
    Verify that statements corresponding to attaching service levels correctly format
    unicode characters, i.e. identifiers like that should be wrapped in quotation marks.
    """

    with AuthSLContext(cql):
        role = '"私の猫"'
        sl = '"サービスレベル"'

        cql.execute(f"CREATE ROLE {role}")
        cql.execute(f"CREATE SERVICE_LEVEL {sl}")
        cql.execute(f"ATTACH SERVICE_LEVEL {sl} TO {role}")

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_attached_service_levels(desc_iter)

        desc_elements = list(desc_iter)

        sl_iter = map(lambda row: row.name, desc_elements)
        assert list(sl_iter) == [sl]

        desc_iter = extract_create_statements(desc_elements)
        assert list(desc_iter) == [f"ATTACH SERVICE_LEVEL {sl} TO {role};"]

def test_desc_auth_attach_service_levels(cql):
    """
    Verify that the output of `DESC SCHEMA WITH INTERNALS` corresponding to attaching service levels
    is as expected for various different cases.
    """

    with AuthSLContext(cql):
        [r1, r2] = ["andrew", '"j@n3 is my fr!3nd"']
        cql.execute(f"CREATE ROLE {r1}")
        cql.execute(f"CREATE ROLE {r2}")
        cql.execute(f"GRANT {r1} TO {r2}")

        [sl1, sl2] = ["my_service_level", '"s0m3 0th3r s3rv!c3 l3v3l!"']
        # Note: The smaller timeout, the better. We also want to verify in this test
        #       that the service level statement returned by `DESC SCHEMA` that corresponds
        #       to `r1` is the actual service level it was granted, not its effective service level.
        cql.execute(f"CREATE SERVICE_LEVEL {sl1} WITH TIMEOUT = 1ms")
        cql.execute(f"CREATE SERVICE_LEVEL {sl2} WITH TIMEOUT = 10ms")

        sl_stmt1 = f"ATTACH SERVICE_LEVEL {sl1} TO {r1};"
        sl_stmt2 = f"ATTACH SERVICE_LEVEL {sl2} TO {r2};"
        sl_stmts = [sl_stmt1, sl_stmt2]

        for stmt in sl_stmts:
            cql.execute(stmt)

        desc_iter = cql.execute("DESC SCHEMA WITH INTERNALS")
        desc_iter = filter_attached_service_levels(desc_iter)
        desc_iter = extract_create_statements(desc_iter)

        assert set(sl_stmts) == set(desc_iter)

def test_desc_restore_auth_and_service_levels(cql):
    """
    Verify that restoring auth and service levels works correctly. We check the following things:
      - applying the statements `DESC SCHEMA WITH INTERNALS AND PASSWORDS` doesn't produce errors,
      - trying to describe the restored schema produces exactly the same output as the original schema.
    """

    def get_auth_and_sl_restore_stmts(desc_rows: Set[DescRowType]) -> list[str]:
        roles_iter = extract_create_statements(filter_roles(desc_rows))
        role_grants_iter = extract_create_statements(filter_grant_roles(desc_rows))
        permissions_iter = extract_create_statements(filter_grant_permissions(desc_rows))
        service_levels_iter = extract_create_statements(filter_service_levels(desc_rows))
        attach_service_levels_iter = extract_create_statements(filter_attached_service_levels(desc_rows))

        return [*roles_iter, *role_grants_iter, *permissions_iter, *service_levels_iter, *attach_service_levels_iter]

    restore_stmts = None
    ks = "my_ks"

    with AuthSLContext(cql, ks=ks):
        [r1, r2, r3] = ["jack", "'b0b @nd d0b!'", "jane"]
        cql.execute(f"CREATE ROLE {r1}")
        cql.execute(f"CREATE ROLE {r2}")
        cql.execute(f"CREATE ROLE {r3}")

        cql.execute(f"GRANT {r1} TO {r3}")
        cql.execute(f"GRANT {r3} TO {r2}")

        cql.execute(f"GRANT ALL ON ALL KEYSPACES TO {r2}")
        cql.execute(f"GRANT SELECT ON KEYSPACE {ks} TO {r3}")
        cql.execute(f"GRANT MODIFY ON TABLE system.roles TO {r1}")
        cql.execute(f"GRANT AUTHORIZE ON ALL ROLES TO {r1}")
        cql.execute(f"GRANT DESCRIBE ON ALL ROLES TO {r1}")

        [sl1, sl2] = ["my_service_level", "'s3rv!c3 l3v3l !!!'"]
        cql.execute(f"CREATE SERVICE_LEVEL {sl1} WITH TIMEOUT = 10ms AND WORKLOAD_TYPE = 'batch'")
        cql.execute(f"CREATE SERVICE_LEVEL {sl2} WITH TIMEOUT = 100s")

        cql.execute(f"ATTACH SERVICE_LEVEL {sl1} TO {r1}")
        cql.execute(f"ATTACH SERVICE_LEVEL {sl1} TO {r2}")
        cql.execute(f"ATTACH SERVICE_LEVEL {sl2} TO {r3}")

        restore_stmts = set(cql.execute("DESC SCHEMA WITH INTERNALS AND PASSWORDS"))

    with AuthSLContext(cql, ks=ks):
        for stmt in get_auth_and_sl_restore_stmts(restore_stmts):
            print(f"Stmt = {stmt}")
            cql.execute(stmt)

        assert restore_stmts == set(cql.execute("DESC SCHEMA WITH INTERNALS AND PASSWORDS"))
