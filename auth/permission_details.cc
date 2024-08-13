/*
 * Copyright (C) 2024-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "auth/permission_details.hh"

// Seastar features.
#include <seastar/coroutine/maybe_yield.hh>

// Scylla includes.
#include "auth/permission.hh"
#include "auth/resource.hh"
#include "cql3/util.hh"
#include "log.hh"

// STD.
#include <ranges>
#include <string_view>

namespace auth {

namespace {

logging::logger pdlogger{"permission_details"};

// The function doesn't assume anything about `role`.
sstring describe_data(const permission& perm, const resource& r, std::string_view role) {
    const auto permission = permissions::to_string(perm);
    const auto formatted_role = cql3::util::maybe_quote(role);

    const auto view = data_resource_view(r);
    const auto maybe_ks = view.keyspace();
    const auto maybe_cf = view.table();

    // The documentation says:
    //
    //     Both keyspace and table names consist of only alphanumeric characters, cannot be empty,
    //     and are limited in size to 48 characters (that limit exists mostly to avoid filenames,
    //     which may include the keyspace and table name, to go over the limits of certain file systems).
    //     By default, keyspace and table names are case insensitive (myTable is equivalent to mytable),
    //     but case sensitivity can be forced by using double-quotes ("myTable" is different from mytable).
    //
    // That's why we wrap identifiers with quotation marks below.

    if (!maybe_ks) {
        return seastar::format("GRANT {} ON ALL KEYSPACES TO {};", permission, formatted_role);
    }
    const auto ks = cql3::util::maybe_quote(*maybe_ks);

    if (!maybe_cf) {
        return seastar::format("GRANT {} ON KEYSPACE {} TO {};", permission, ks, formatted_role);
    }
    const auto cf = cql3::util::maybe_quote(*maybe_cf);

    return seastar::format("GRANT {} ON {}.{} TO {};", permission, ks, cf, formatted_role);
}

// The function doesn't assume anything about `role`.
sstring describe_role(const permission& perm, const resource& r, std::string_view role) {
    const auto permission = permissions::to_string(perm);
    const auto formatted_role = cql3::util::maybe_quote(role);

    const auto view = role_resource_view(r);
    const auto maybe_target_role = view.role();

    if (!maybe_target_role) {
        return seastar::format("GRANT {} ON ALL ROLES TO {};", permission, formatted_role);
    }
    return seastar::format("GRANT {} ON ROLE {} TO {};", permission, cql3::util::maybe_quote(*maybe_target_role), formatted_role);
}

// The function doesn't assume anything about `role`.
sstring describe_service_level(const permission& perm, const resource& r, std::string_view role) {
    on_internal_error(pdlogger, "Granting permissions for service levels is not supported");
}

// The function doesn't assume anything about `role`.
sstring describe_udf(const permission& perm, const resource& r, std::string_view role) {
    const auto permission = permissions::to_string(perm);
    const auto formatted_role = cql3::util::maybe_quote(role);

    const auto view = functions_resource_view(r);
    const auto maybe_ks = view.keyspace();
    const auto maybe_fun_sig = view.function_signature();
    const auto maybe_fun_name = view.function_name();
    const auto maybe_fun_args = view.function_args();

    // The documentation says:
    //
    //     Both keyspace and table names consist of only alphanumeric characters, cannot be empty,
    //     and are limited in size to 48 characters (that limit exists mostly to avoid filenames,
    //     which may include the keyspace and table name, to go over the limits of certain file systems).
    //     By default, keyspace and table names are case insensitive (myTable is equivalent to mytable),
    //     but case sensitivity can be forced by using double-quotes ("myTable" is different from mytable).
    //
    // That's why we wrap identifiers with quotation marks below.

    if (!maybe_ks) {
        return seastar::format("GRANT {} ON ALL FUNCTIONS TO {};", permission, formatted_role);
    }
    const auto ks = cql3::util::maybe_quote(*maybe_ks);

    if (!maybe_fun_sig && !maybe_fun_name) {
        return seastar::format("GRANT {} ON ALL FUNCTIONS IN KEYSPACE {} TO {};", permission, ks, formatted_role);
    }

    if (maybe_fun_name) {
        SCYLLA_ASSERT(maybe_fun_args);

        const auto fun_name = cql3::util::maybe_quote(*maybe_fun_name);
        const auto fun_args_range = *maybe_fun_args | std::views::transform([] (const auto& fun_arg) {
            return cql3::util::maybe_quote(fun_arg);
        });

        return seastar::format("GRANT {} ON FUNCTION {}.{}({}) TO {};",
                permission, ks, fun_name, fmt::join(fun_args_range, ", "), formatted_role);
    }

    SCYLLA_ASSERT(maybe_fun_sig);

    auto [fun_name, fun_args] = decode_signature(*maybe_fun_sig);
    fun_name = cql3::util::maybe_quote(fun_name);

    // We don't call `cql3::util::maybe_quote` later because `cql3_type_name_without_frozen` already guarantees
    // that the type will be wrapped within double quotation marks if it's necessary.
    auto parsed_fun_args = fun_args | std::views::transform([] (const data_type& dt) {
        return dt->without_reversed().cql3_type_name_without_frozen();
    });

    return seastar::format("GRANT {} ON FUNCTION {}.{}({}) TO {};",
            permission, ks, fun_name, fmt::join(parsed_fun_args, ", "), formatted_role);
}

// The function doesn't assume anything about `role`.
sstring describe_element_kind(const permission& perm, const resource& r, std::string_view role) {
    switch (r.kind()) {
        case resource_kind::data:
            return describe_data(perm, r, role);
        case resource_kind::role:
            return describe_role(perm, r, role);
        case resource_kind::service_level:
            return describe_service_level(perm, r, role);
        case resource_kind::functions:
            return describe_udf(perm, r, role);
    }
}

} // anonymous namespace

future<std::vector<std::pair<sstring, sstring>>> permission_details::describe_entities(bool with_internals) const {
    std::vector<std::pair<sstring, sstring>> result{};

    for (const permission& perm : permissions) {
        result.emplace_back(role_name, describe_element_kind(perm, resource, role_name));
        co_await coroutine::maybe_yield();
    }

    co_return result;
}

} // namespace auth
