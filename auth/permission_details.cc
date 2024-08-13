/*
 * Copyright (C) 2024-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#include "auth/permission_details.hh"

// FMT.
#include <fmt/format.h>

// Scylla includes.
#include "auth/permission.hh"
#include "auth/resource.hh"
#include "log.hh"

// STD.
#include <ranges>
#include <string_view>

namespace auth {

namespace {

logging::logger pdlogger{"permission_details"};

sstring describe_data(std::string_view permission, const resource& r, std::string_view role) {
    constexpr std::string_view ALL_KS_FMT_STMT = "GRANT {} ON ALL KEYSPACES TO {};";
    constexpr std::string_view KS_FMT_STMT = "GRANT {} ON KEYSPACE {} TO {};";
    constexpr std::string_view CF_FMT_STMT = "GRANT {} ON {}.{} TO {};";

    const auto view = data_resource_view(r);
    const auto maybe_ks = view.keyspace();
    const auto maybe_cf = view.table();

    // TODO: Make sure that ks/cf cannot have weird characters.
    if (!maybe_ks) {
        return fmt::format(ALL_KS_FMT_STMT, permission, role);
    }
    if (!maybe_cf) {
        return fmt::format(KS_FMT_STMT, permission, *maybe_ks, role);
    }
    return fmt::format(CF_FMT_STMT, permission, *maybe_ks, *maybe_cf, role);
}

sstring describe_role(std::string_view permission, const resource& r, std::string_view role) {
    constexpr std::string_view ALL_ROLE_FMT_STMT = "GRANT {} ON ALL USERS TO {};";
    constexpr std::string_view ROLE_FMT_STMT = "GRANT {} ON USER {} TO {};";

    const auto view = role_resource_view(r);
    const auto maybe_target_role = view.role();

    // TODO: Make sure that roles cannot have weird characters.
    if (!maybe_target_role) {
        return fmt::format(ALL_ROLE_FMT_STMT, permission, role);
    }
    return fmt::format(ROLE_FMT_STMT, permission, *maybe_target_role, role);
}

sstring describe_service_level(std::string_view permission, const resource& r, std::string_view role) {
    on_internal_error(pdlogger, "Granting permissions for service levels is not supported");
}

sstring describe_udf(std::string_view permission, const resource& r, std::string_view role) {
    constexpr std::string_view ALL_FUNCS_FMT_STMT = "GRANT {} ON ALL FUNCTIONS TO {};";
    constexpr std::string_view ALL_FUNCS_IN_KS_FMT_STMT = "GRANT {} ON ALL FUNCTIONS IN {} TO {};";

    // TODO: Verify this is the right syntax.
    constexpr std::string_view FUN_IN_KS_FMT_STMT = "GRANT {} ON {}.{}({}) TO {};";

    const auto view = functions_resource_view(r);
    const auto maybe_ks = view.keyspace();
    const auto maybe_fun_sig = view.function_signature();
    auto maybe_fun_name = view.function_name();
    auto maybe_fun_args = view.function_args();

    if (!maybe_ks) {
        return fmt::format(ALL_FUNCS_FMT_STMT, permission, role);
    }
    if (!maybe_fun_sig && !maybe_fun_name) {
        return fmt::format(ALL_FUNCS_IN_KS_FMT_STMT, permission, *maybe_ks, role);
    }

    if (maybe_fun_name) {
        SCYLLA_ASSERT(maybe_fun_args);
        return fmt::format(FUN_IN_KS_FMT_STMT, permission, *maybe_ks, *maybe_fun_name,
                fmt::join(*maybe_fun_args, ", "), role);
    }

    SCYLLA_ASSERT(maybe_fun_sig);
    auto [fun_name, fun_args] = decode_signature(*maybe_fun_sig);
    auto parsed_fun_args = fun_args | std::views::transform([] (const data_type& dt) {
        return dt->cql3_type_name();
    });

    return fmt::format(FUN_IN_KS_FMT_STMT, permission, *maybe_ks, fun_name, fmt::join(parsed_fun_args, ", "), role);
}

sstring describe_element_kind(std::string_view permission, const resource& r, std::string_view role) {
    switch (r.kind()) {
        case resource_kind::data:
            return describe_data(permission, r, role);
        case resource_kind::role:
            return describe_role(permission, r, role);
        case resource_kind::service_level:
            return describe_service_level(permission, r, role);
        case resource_kind::functions:
            return describe_udf(permission, r, role);
    }
}

} // anonymous namespace

std::vector<std::pair<sstring, sstring>> permission_details::describe_elements([[maybe_unused]] const replica::database&,
        [[maybe_unused]] bool with_internals) const
{
    std::vector<std::pair<sstring, sstring>> result{};

    // If ALL of the permissions are granted to a resource, we can reduce the output to one CQL statement.
    if (permissions.mask() == permission_set::full_mask()) {
        result.reserve(1);
        result.emplace_back(role_name, describe_element_kind("ALL", resource, role_name));
        return result;
    }

    const auto permission_strings = permissions::to_strings(permissions);
    result.reserve(permission_strings.size());

    for (const auto& permission_string : permission_strings) {
        result.emplace_back(role_name, describe_element_kind(permission_string, resource, role_name));
    }

    return result;
}

} // namespace auth
