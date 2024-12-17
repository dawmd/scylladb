/*
 * Copyright (C) 2016-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */


#include <boost/range/irange.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/test/unit_test.hpp>
#include <stdint.h>
#include <fmt/ranges.h>

#include <seastar/core/future.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/thread.hh>

#include "cql3/CqlParser.hpp"
#include "exceptions/exceptions.hh"
#include "service/raft/raft_group0_client.hh"
#include "test/lib/scylla_test_case.hh"
#include "test/lib/cql_test_env.hh"
#include "test/lib/cql_assertions.hh"
#include "test/lib/exception_utils.hh"

#include "auth/allow_all_authenticator.hh"
#include "auth/authenticator.hh"
#include "auth/password_authenticator.hh"
#include "auth/service.hh"
#include "auth/authenticated_user.hh"

#include "db/config.hh"

cql_test_config auth_on(bool with_authorizer = true) {
    cql_test_config cfg;
    if (with_authorizer) {
        cfg.db_config->authorizer("CassandraAuthorizer");
    }
    cfg.db_config->authenticator("PasswordAuthenticator");
    return cfg;
}

SEASTAR_TEST_CASE(test_default_authenticator) {
    return do_with_cql_env([](cql_test_env& env) {
        auto& a = env.local_auth_service().underlying_authenticator();
        BOOST_REQUIRE(!a.require_authentication());
        BOOST_REQUIRE_EQUAL(a.qualified_java_name(), auth::allow_all_authenticator_name);
        return make_ready_future();
    });
}

SEASTAR_TEST_CASE(test_password_authenticator_attributes) {
    return do_with_cql_env([](cql_test_env& env) {
        auto& a = env.local_auth_service().underlying_authenticator();
        BOOST_REQUIRE(a.require_authentication());
        BOOST_REQUIRE_EQUAL(a.qualified_java_name(), auth::password_authenticator_name);
        return make_ready_future();
    }, auth_on(false));
}

static future<auth::authenticated_user>
authenticate(cql_test_env& env, std::string_view username, std::string_view password) {
    auto& c = env.local_client_state();
    auto& a = env.local_auth_service().underlying_authenticator();

    return do_with(
            auth::authenticator::credentials_map{
                    {auth::authenticator::USERNAME_KEY, sstring(username)},
                    {auth::authenticator::PASSWORD_KEY, sstring(password)}},
            [&a, &c](const auto& credentials) {
        return a.authenticate(credentials).then([&c](auth::authenticated_user u) {
            c.set_login(std::move(u));
            return c.check_user_can_login().then([&c] { return *c.user(); });
        });
    });
}

template <typename Exception, typename... Args>
future<> require_throws(seastar::future<Args...> fut) {
    return fut.then_wrapped([](auto completed_fut) {
        try {
            completed_fut.get();
            BOOST_FAIL("Required an exception to be thrown");
        } catch (const Exception&) {
            // Ok.
        }
    });
}

SEASTAR_TEST_CASE(test_password_authenticator_operations) {
    /**
     * Not using seastar::async due to apparent ASan bug.
     * Enjoy the slightly less readable code.
     */
    return do_with_cql_env([](cql_test_env& env) {
        static const sstring username("fisk");
        static const sstring password("notter");

        // check non-existing user
        return require_throws<exceptions::authentication_exception>(
            authenticate(env, username, password)).then([&env] {
            return seastar::async([&env] () {
                cquery_nofail(env, format("CREATE ROLE {} WITH PASSWORD = '{}' AND LOGIN = true", username, password));
            }).then([&env] () {
                return authenticate(env, username, password);
            }).then([] (auth::authenticated_user user) {
                BOOST_REQUIRE(!auth::is_anonymous(user));
                BOOST_REQUIRE_EQUAL(*user.name, username);
            });
        }).then([&env] {
            return require_throws<exceptions::authentication_exception>(authenticate(env, username, "hejkotte"));
        }).then([&env] {
            //
            // A role must be explicitly marked as being allowed to log in.
            //

            return do_with(
                    auth::role_config_update{},
                    auth::authentication_options{},
                    [&env](auto& config_update, const auto& options) {
                config_update.can_login = false;

                return seastar::async([&env] {
                    do_with_mc(env, [&env] (auto& mc) {
                        auth::authentication_options opts;
                        auth::role_config_update conf;
                        conf.can_login = false;
                        auth::alter_role(env.local_auth_service(), username, conf, opts, mc).get();
                    });
                    // has to be in a separate transaction to observe results of alter role
                    do_with_mc(env, [&env] (auto& mc) {
                        require_throws<exceptions::authentication_exception>(authenticate(env, username, password)).get();
                    });
                });
            });
        }).then([&env] {
            // sasl
            auto& a = env.local_auth_service().underlying_authenticator();
            auto sasl = a.new_sasl_challenge();

            BOOST_REQUIRE(!sasl->is_complete());

            bytes b;
            int8_t i = 0;
            b.append(&i, 1);
            b.insert(b.end(), username.begin(), username.end());
            b.append(&i, 1);
            b.insert(b.end(), password.begin(), password.end());

            sasl->evaluate_response(b);
            BOOST_REQUIRE(sasl->is_complete());

            return sasl->get_authenticated_user().then([](auth::authenticated_user user) {
                BOOST_REQUIRE(!auth::is_anonymous(user));
                BOOST_REQUIRE_EQUAL(*user.name, username);
            });
        }).then([&env] {
            // check deleted user
            return seastar::async([&env] {
                do_with_mc(env, [&env] (auto& mc) {
                    auth::drop_role(env.local_auth_service(), username, mc).get();
                    require_throws<exceptions::authentication_exception>(authenticate(env, username, password)).get();
                });
            });
        });
    }, auth_on(false));
}

namespace {

/// Asserts that table is protected from alterations that can brick a node.
void require_table_protected(cql_test_env& env, const char* table) {
    using exception_predicate::message_matches;
    using unauth = exceptions::unauthorized_exception;
    const auto q = [&] (const char* stmt) { return env.execute_cql(fmt::format(fmt::runtime(stmt), table)).get(); };
    const char* pattern = ".*(is protected)|(is not user-modifiable).*";
    BOOST_TEST_INFO(table);
    BOOST_REQUIRE_EXCEPTION(q("ALTER TABLE {} ALTER role TYPE blob"), unauth, message_matches(pattern));
    BOOST_REQUIRE_EXCEPTION(q("ALTER TABLE {} RENAME role TO user"), unauth, message_matches(pattern));
    BOOST_REQUIRE_EXCEPTION(q("ALTER TABLE {} DROP role"), unauth, message_matches(pattern));
    BOOST_REQUIRE_EXCEPTION(q("DROP TABLE {}"), unauth, message_matches(pattern));
}

} // anonymous namespace

SEASTAR_TEST_CASE(roles_table_is_protected) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        require_table_protected(env, "system.roles");
    }, auth_on());
}

SEASTAR_TEST_CASE(role_members_table_is_protected) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        require_table_protected(env, "system.role_members");
    }, auth_on());
}

SEASTAR_TEST_CASE(role_permissions_table_is_protected) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        require_table_protected(env, "system.role_permissions");
    }, auth_on());
}

SEASTAR_TEST_CASE(test_alter_with_timeouts) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "CREATE ROLE user1 WITH PASSWORD = 'pass' AND LOGIN = true");
        cquery_nofail(e, "CREATE ROLE user2 WITH PASSWORD = 'pass' AND LOGIN = true");
        cquery_nofail(e, "CREATE ROLE user3 WITH PASSWORD = 'pass' AND LOGIN = true");
        cquery_nofail(e, "CREATE ROLE user4 WITH PASSWORD = 'pass' AND LOGIN = true");
        authenticate(e, "user1", "pass").get();

        cquery_nofail(e, "CREATE SERVICE LEVEL sl WITH timeout = 5ms");
        cquery_nofail(e, "ATTACH SERVICE LEVEL sl TO user1");

        cquery_nofail(e, "CREATE TABLE t (id int, v int, PRIMARY KEY(id, v))");
        cquery_nofail(e, "INSERT INTO t (id, v) VALUES (1, 2)");
        cquery_nofail(e, "INSERT INTO t (id, v) VALUES (2, 3)");
        cquery_nofail(e, "INSERT INTO t (id, v) VALUES (3, 4)");
        // Avoid reading from memtables, which does not check timeouts due to being too fast
        e.db().invoke_on_all([] (replica::database& db) { return db.flush_all_memtables(); }).get();

        auto sl_is_v2 = e.local_client_state().get_service_level_controller().is_v2();
	    auto msg = cquery_nofail(e, format("SELECT timeout FROM {}", sl_is_v2 ? "system.service_levels_v2" : "system_distributed.service_levels"));
        assert_that(msg).is_rows().with_rows({{
            duration_type->from_string("5ms")
        }});

        cquery_nofail(e, "ALTER SERVICE LEVEL sl WITH timeout = 35s");

	    msg = cquery_nofail(e, format("SELECT timeout FROM {} WHERE service_level = 'sl'", sl_is_v2 ? "system.service_levels_v2" : "system_distributed.service_levels"));
        assert_that(msg).is_rows().with_rows({{
            duration_type->from_string("35s")
        }});

        // Setting a timeout value of 0 makes little sense, but it's great for testing
        cquery_nofail(e, "ALTER SERVICE LEVEL sl WITH timeout = 0s");
        e.refresh_client_state().get();
        BOOST_REQUIRE_THROW(e.execute_cql("SELECT * FROM t BYPASS CACHE").get(), exceptions::read_timeout_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("INSERT INTO t (id, v) VALUES (1,2)").get(), exceptions::mutation_write_timeout_exception);

        cquery_nofail(e, "ALTER SERVICE LEVEL sl WITH timeout = null");
        e.refresh_client_state().get();
        cquery_nofail(e, "SELECT * FROM t BYPASS CACHE");
        cquery_nofail(e, "INSERT INTO t (id, v) VALUES (1,2)");

        // Only valid timeout values are accepted
        BOOST_REQUIRE_THROW(e.execute_cql("ALTER SERVICE LEVEL sl WITH timeout = 'I am not a valid duration'").get(), exceptions::syntax_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("ALTER SERVICE LEVEL sl WITH timeout = 5us").get(), exceptions::invalid_request_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("ALTER SERVICE LEVEL sl WITH timeout = 2y6mo5d").get(), exceptions::invalid_request_exception);

        // When multiple per-role timeouts apply, the smallest value is always effective
        cquery_nofail(e, "CREATE SERVICE LEVEL sl2 WITH timeout = 2s");
        cquery_nofail(e, "CREATE SERVICE LEVEL sl3 WITH timeout = 0s");
        cquery_nofail(e, "CREATE SERVICE LEVEL sl4 WITH timeout = 3s");
        cquery_nofail(e, "ATTACH SERVICE LEVEL sl2 TO user2");
        cquery_nofail(e, "ATTACH SERVICE LEVEL sl3 TO user3");
        cquery_nofail(e, "ATTACH SERVICE LEVEL sl4 TO user4");
        cquery_nofail(e, "ALTER SERVICE LEVEL sl WITH timeout = 5s");
        // The roles are granted as follows:
         //  user4  user3
        //      \ /
        //      user2
        //      /
        //    user1
        //
        // which means that user1 should inherit timeouts from all other users
        cquery_nofail(e, "GRANT user2 TO user1"); 
        cquery_nofail(e, "GRANT user3 TO user2");
        cquery_nofail(e, "GRANT user4 TO user2");
        e.refresh_client_state().get();
        // Avoid reading from memtables, which does not check timeouts due to being too fast
        e.db().invoke_on_all([] (replica::database& db) { return db.flush_all_memtables(); }).get();
        // For user1, operations should time out
        BOOST_REQUIRE_THROW(e.execute_cql("SELECT * FROM t where id = 1 BYPASS CACHE").get(), exceptions::read_timeout_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("SELECT * FROM t BYPASS CACHE").get(), exceptions::read_timeout_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("INSERT INTO t (id, v) VALUES (1,2)").get(), exceptions::mutation_write_timeout_exception);
        // after switching to user2, same thing should be observed
        authenticate(e, "user2", "pass").get();
        e.refresh_client_state().get();
        BOOST_REQUIRE_THROW(e.execute_cql("SELECT * FROM t where id = 1 BYPASS CACHE").get(), exceptions::read_timeout_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("SELECT * FROM t BYPASS CACHE").get(), exceptions::read_timeout_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("INSERT INTO t (id, v) VALUES (1,2)").get(), exceptions::mutation_write_timeout_exception);
        // after switching to user3, same thing should be observed
        authenticate(e, "user3", "pass").get();
        e.refresh_client_state().get();
        BOOST_REQUIRE_THROW(e.execute_cql("SELECT * FROM t where id = 1 BYPASS CACHE").get(), exceptions::read_timeout_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("SELECT * FROM t BYPASS CACHE").get(), exceptions::read_timeout_exception);
        BOOST_REQUIRE_THROW(e.execute_cql("INSERT INTO t (id, v) VALUES (1,2)").get(), exceptions::mutation_write_timeout_exception);
        // after switching to user4, everything should work fine
        authenticate(e, "user4", "pass").get();
        e.refresh_client_state().get();
        cquery_nofail(e, "SELECT * FROM t where id = 1 BYPASS CACHE");
        cquery_nofail(e, "SELECT * FROM t BYPASS CACHE");
        cquery_nofail(e, "INSERT INTO t (id, v) VALUES (1,2)");
    }, auth_on(false));
}

SEASTAR_TEST_CASE(test_alter_with_workload_type) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "CREATE ROLE user1 WITH PASSWORD = 'pass' AND LOGIN = true");
        cquery_nofail(e, "CREATE ROLE user2 WITH PASSWORD = 'pass' AND LOGIN = true");
        cquery_nofail(e, "CREATE ROLE user3 WITH PASSWORD = 'pass' AND LOGIN = true");
        cquery_nofail(e, "CREATE ROLE user4 WITH PASSWORD = 'pass' AND LOGIN = true");
        authenticate(e, "user1", "pass").get();

        cquery_nofail(e, "CREATE SERVICE LEVEL sl");
        cquery_nofail(e, "ATTACH SERVICE LEVEL sl TO user1");

        auto sl_is_v2 = e.local_client_state().get_service_level_controller().is_v2();
	    auto msg = cquery_nofail(e, format("SELECT workload_type FROM {}", sl_is_v2 ? "system.service_levels_v2" : "system_distributed.service_levels"));
        assert_that(msg).is_rows().with_rows({{
            {}
        }});

        e.refresh_client_state().get();
        // Default workload type is `unspecified`
        BOOST_REQUIRE_EQUAL(e.local_client_state().get_workload_type(), service::client_state::workload_type::unspecified);

        // When multiple per-role timeouts apply, the smallest value is always effective
        cquery_nofail(e, "CREATE SERVICE LEVEL sl2 WITH workload_type = null");
        cquery_nofail(e, "CREATE SERVICE LEVEL sl3 WITH workload_type = 'batch'");
        cquery_nofail(e, "CREATE SERVICE LEVEL sl4 WITH workload_type = 'interactive'");
        cquery_nofail(e, "ATTACH SERVICE LEVEL sl2 TO user2");
        cquery_nofail(e, "ATTACH SERVICE LEVEL sl3 TO user3");
        cquery_nofail(e, "ATTACH SERVICE LEVEL sl4 TO user4");
        cquery_nofail(e, "ALTER SERVICE LEVEL sl WITH workload_type = 'interactive'");
        // The roles are granted as follows:
         //  user4  user3
        //      \ /
        //      user2
        //      /
        //    user1
        //
        // which means that user1 should inherit workload types from all other users
        cquery_nofail(e, "GRANT user2 TO user1"); 
        cquery_nofail(e, "GRANT user3 TO user2");
        cquery_nofail(e, "GRANT user4 TO user2");
        e.refresh_client_state().get();
        // For user1, the effective workload type should be batch
        BOOST_REQUIRE_EQUAL(e.local_client_state().get_workload_type(), service::client_state::workload_type::batch);
        // after switching to user2, still batch
        authenticate(e, "user2", "pass").get();
        e.refresh_client_state().get();
        BOOST_REQUIRE_EQUAL(e.local_client_state().get_workload_type(), service::client_state::workload_type::batch);
        // after switching to user3, batch again
        authenticate(e, "user3", "pass").get();
        e.refresh_client_state().get();
        BOOST_REQUIRE_EQUAL(e.local_client_state().get_workload_type(), service::client_state::workload_type::batch);
        // after switching to user4, the workload is interactive
        authenticate(e, "user4", "pass").get();
        e.refresh_client_state().get();
        BOOST_REQUIRE_EQUAL(e.local_client_state().get_workload_type(), service::client_state::workload_type::interactive);
    }, auth_on(false));
}

SEASTAR_TEST_CASE(test_try_to_create_role_with_hashed_password_and_password) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        BOOST_REQUIRE_THROW(
            env.execute_cql("CREATE ROLE jane WITH HASHED PASSWORD = 'something' AND PASSWORD = 'something'").get(),
            exceptions::syntax_exception);
    }, auth_on(false));
}

SEASTAR_TEST_CASE(test_try_to_create_role_with_password_and_hashed_password) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        BOOST_REQUIRE_THROW(
            env.execute_cql("CREATE ROLE jane WITH PASSWORD = 'something' AND HASHED PASSWORD = 'something'").get(),
            exceptions::syntax_exception);
    }, auth_on(false));
}

SEASTAR_TEST_CASE(test_try_create_roles_with_invalid_hashed_passwords) {
    // This test ensures that Scylla correctly verifies that the provided hashed password
    // follows the format of the supported algorithms. It uses invalid examples for that purpose.
    return do_with_cql_env_thread([] (cql_test_env& env) {
        constexpr std::string_view hashed_passwords[] = {
            // bcrypt with an additional character.
            "$2a$10$F5pRau9mKg5abP.DsuPQl.8rQpEoNm3OV91mKjb9vdKPUPejIPq/ua",
            // "Almost" bcrypt -- wrong prefix.
            "$2u$10$F5pRau9mKg5abP.DsuPQl.8rQpEoNm3OV91mKjb9vdKPUPejIPq/u",
            "$2$10$F5pRau9mKg5abP.DsuPQl.8rQpEoNm3OV91mKjb9vdKPUPejIPq/u",
            "$2a10$F5pRau9mKg5abP.DsuPQl.8rQpEoNm3OV91mKjb9vdKPUPejIPq/u",
            "2a$10$F5pRau9mKg5abP.DsuPQl.8rQpEoNm3OV91mKjb9vdKPUPejIPq/u",
            // sha512 with an additional character.
            "$6$shzqPMxwOZxPvh7G$H65LkotwT6zjkJ2eXsGPGtVcoOzttO9VTIVgb4BMECMLTSXo4cSgDq3G2sPG9t2itQGIff0LEHvLFqMBrJO5x/a",
            // "Almost" sha512 -- wrong prefix.
            "$7$shzqPMxwOZxPvh7G$H65LkotwT6zjkJ2eXsGPGtVcoOzttO9VTIVgb4BMECMLTSXo4cSgDq3G2sPG9t2itQGIff0LEHvLFqMBrJO5x/",
            "$6a$shzqPMxwOZxPvh7G$H65LkotwT6zjkJ2eXsGPGtVcoOzttO9VTIVgb4BMECMLTSXo4cSgDq3G2sPG9t2itQGIff0LEHvLFqMBrJO5x/",
            "$6shzqPMxwOZxPvh7G$H65LkotwT6zjkJ2eXsGPGtVcoOzttO9VTIVgb4BMECMLTSXo4cSgDq3G2sPG9t2itQGIff0LEHvLFqMBrJO5x/",
            "6$shzqPMxwOZxPvh7G$H65LkotwT6zjkJ2eXsGPGtVcoOzttO9VTIVgb4BMECMLTSXo4cSgDq3G2sPG9t2itQGIff0LEHvLFqMBrJO5x/",
            // sha512 with a negative number of rounds.
            "$6$rounds=-50000$RzOKKJIKZ18arhBR$E2JyKkCOZZXva89rAzvQOZxL1rZ.1wt9uC4yEVr0YkZkTbpi1bd7QBexgpbbc5qI4NvntJQAlrYSPg2bSvVr5/",
            // sha512 with zero rounds.
            "$6$rounds=0$RzOKKJIKZ18arhBR$E2JyKkCOZZXva89rAzvQOZxL1rZ.1wt9uC4yEVr0YkZkTbpi1bd7QBexgpbbc5qI4NvntJQAlrYSPg2bSvVr5/",
            // sha512 with a number of rounds starting with a zero.
            "$6$rounds=050000$RzOKKJIKZ18arhBR$E2JyKkCOZZXva89rAzvQOZxL1rZ.1wt9uC4yEVr0YkZkTbpi1bd7QBexgpbbc5qI4NvntJQAlrYSPg2bSvVr5/",
            // sha256 with an additional character.
            "$5$XBEj8oXL9aXtt/Kg$1PPbUfUfJkFjZKO0YfgT/6UGaxiK9SnEALdPY5FQs46a",
            // "Almost" sha256 -- wrong prefix.
            "$7$XBEj8oXL9aXtt/Kg$1PPbUfUfJkFjZKO0YfgT/6UGaxiK9SnEALdPY5FQs46",
            "$5a$XBEj8oXL9aXtt/Kg$1PPbUfUfJkFjZKO0YfgT/6UGaxiK9SnEALdPY5FQs46",
            "$5XBEj8oXL9aXtt/Kg$1PPbUfUfJkFjZKO0YfgT/6UGaxiK9SnEALdPY5FQs46",
            "5$XBEj8oXL9aXtt/Kg$1PPbUfUfJkFjZKO0YfgT/6UGaxiK9SnEALdPY5FQs46",
            // sha256 with a negative number of rounds.
            "$5$rounds=-10000$usesomesillystri$CaVZTpnmHfz9Fi6zjGJmSAAwwABsSqSJbtzdlfvVC10",
            // sha256 with zero rounds.
            "$5$rounds=0$usesomesillystri$CaVZTpnmHfz9Fi6zjGJmSAAwwABsSqSJbtzdlfvVC10",
            // sha256 with a number of rounds starting with a zero.
            "$5$rounds=01000$usesomesillystri$CaVZTpnmHfz9Fi6zjGJmSAAwwABsSqSJbtzdlfvVC10",
            // md5 with an additional character.
            "$1$Hdl.BeYY$K7qOAGS.CPHAq1X39V7pH1a",
            // md5 with one missing character.
            "$1$Hdl.BeYY$K7qOAGS.CPHAq1X39V7pH",
            // "Almost" md5 -- wrong prefix.
            "$7$Hdl.BeYY$K7qOAGS.CPHAq1X39V7pH1",
            "$1Hdl.BeYY$K7qOAGS.CPHAq1X39V7pH1",
            "1$Hdl.BeYY$K7qOAGS.CPHAq1X39V7pH1"
        };

        for (auto hashed_password : hashed_passwords) {
            auto stmt = seastar::format("CREATE ROLE r WITH HASHED PASSWORD = '{}'", hashed_password);
            BOOST_REQUIRE_EXCEPTION(env.execute_cql(stmt).get(), exceptions::invalid_request_exception,
                    exception_predicate::message_equals("The provided hashed password doesn't use one "
                            "of the supported encryption formats: bcrypt, sha512, sha256, md5"));
        }
    }, auth_on(true));
}

SEASTAR_TEST_CASE(test_try_create_roles_with_nonsupported_encryption_algorithms) {
    // This test ensures that Scylla rejects hashed passwords that use encryption
    // algorithms that are not supported.
    return do_with_cql_env_thread([] (cql_test_env& env) {
        // Hashes corresponding to the remaining encryption algorithms for crypt(3).
        constexpr std::string_view hashed_passwords[] = {
            // yescrypt.
            "$y$j9T$ordrWDeLlAXo0Mka80gLT/$sXffWRhGyBXrMdKrB7hx8p5RwdDM7c0YT8UKBid9P/B",
            // gost-yescrypt.
            "$gy$j9T$qqV5I7w1FhPAoDlQIHJlf/$qE.FfNpywNWVp6BKpqCIcpHIRgN1noSIWEFOw1Q.cBD",
            // scrypt.
            "$7$CU..../....6TAK3eKVS6/HcAOvQizyT1$2QrgnPNlDCwbIzSAZdyA/nJBWPXSVGd5VGEPjklbfL6",
            // sha1.
            "$sha1$205988$oFSTEQ.4mqN5wfGlwNbi$wHeb7GuavUQWh7jCQqI2n0afSO.r",
            // SunMD5.
            "$md5,rounds=59300$6871OZUL$$rx2DlMF/q1kJzGANXzkg3/",
            // bsdicrypt
            "_J9..xXi2CIfeSZRmgv.",
            // descrypt.
            "abcde12345/01",
            // bigcrypt.
            "abcde12345/01abcde12345/01abcde12345/01",
            // NT.
            "$3$$358c649434cc514bf4c8855ad22c826b"
        };


        for (auto hashed_password : hashed_passwords) {
            auto stmt = seastar::format("CREATE ROLE r WITH HASHED PASSWORD = '{}'", hashed_password);
            BOOST_REQUIRE_EXCEPTION(env.execute_cql(stmt).get(), exceptions::invalid_request_exception,
                    exception_predicate::message_equals("The provided hashed password doesn't use one "
                            "of the supported encryption formats: bcrypt, sha512, sha256, md5"));
        }
    }, auth_on(true));
}

SEASTAR_TEST_CASE(test_create_roles_with_hashed_password_and_log_in) {
    // This test ensures that Scylla allows for creating roles with hashed passwords
    // following the format of one of the supported algorithms, as well as logging in
    // as that role is performed successfully.
    return do_with_cql_env_thread([] (cql_test_env& env) {
        // Pairs of form (password, hashed password).
        constexpr std::pair<std::string_view, std::string_view> passwords[] = {
            // bcrypt's.
            {"myPassword", "$2a$05$ae4qyC7lYe47n8K2f/fgKuW/TCRCCpEvcYrA4Dl14VYJAjAEz3tli"},
            {"myPassword", "$2b$05$ae4qyC7lYe47n8K2f/fgKuW/TCRCCpEvcYrA4Dl14VYJAjAEz3tli"},
            {"myPassword", "$2x$05$ae4qyC7lYe47n8K2f/fgKuW/TCRCCpEvcYrA4Dl14VYJAjAEz3tli"},
            {"myPassword", "$2y$05$ae4qyC7lYe47n8K2f/fgKuW/TCRCCpEvcYrA4Dl14VYJAjAEz3tli"},
            // sha512.
            {"myPassword", "$6$pffOF1SkGYpLPe7h$tsYwSqUvbzh2O79dtMNadUsYawCrHMfK06XWFh3vJIMwqaVsaiFsubB2a7uZshDVpJWhTCnGWGKsy3fAteFw9/"},
            // sha256.
            {"myPassword", "$5$AKS.nD1e18H.7gu9$IWy7QB0K.qoYkrWmFn6rZ4BO6Y.FWdCchrFg3beXfx8"},
            // md5.
            {"myPassword", "$1$rVcnG0Et$qAhrrNev1JVV9Zu5qhnry1"}
        };

        for (auto [pwd, hash] : passwords) {
            env.execute_cql(seastar::format("CREATE ROLE r WITH HASHED PASSWORD = '{}' AND LOGIN = true", hash)).get();

            // First, try to log in using an incorrect password.
            BOOST_REQUIRE_EXCEPTION(authenticate(env, "r", "notThePassword").get(), exceptions::authentication_exception,
                    exception_predicate::message_equals("Username and/or password are incorrect"));
            // Now use the correct one.
            authenticate(env, "r", pwd).get();
            
            // We need to log in as a superuser to be able to drop the role.
            authenticate(env, "cassandra", "cassandra").get();
            env.execute_cql("DROP ROLE r").get();
        }
    }, auth_on(true));
}

SEASTAR_TEST_CASE(test_try_create_role_with_hashed_password_as_anonymous_user) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        env.local_client_state().set_login(auth::anonymous_user());
        env.refresh_client_state().get();
        BOOST_REQUIRE(auth::is_anonymous(*env.local_client_state().user()));
        BOOST_REQUIRE_THROW(env.execute_cql("CREATE ROLE my_new_role WITH HASHED PASSWORD = 'myhash'").get(), exceptions::unauthorized_exception);
    }, auth_on(true));
}

SEASTAR_TEST_CASE(test_try_describe_schema_with_internals_and_passwords_as_anonymous_user) {
    return do_with_cql_env_thread([] (cql_test_env& env) {
        env.local_client_state().set_login(auth::anonymous_user());
        env.refresh_client_state().get();
        BOOST_REQUIRE(auth::is_anonymous(*env.local_client_state().user()));
        BOOST_REQUIRE_EXCEPTION(env.execute_cql("DESC SCHEMA WITH INTERNALS AND PASSWORDS").get(), exceptions::unauthorized_exception,
                exception_predicate::message_equals("DESCRIBE SCHEMA WITH INTERNALS AND PASSWORDS can only be issued by a superuser"));
    }, auth_on(true));
}
