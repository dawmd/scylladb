/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "auth/role_info.hh"

// Seastar features.
#include <seastar/core/print.hh>

// Scylla includes.
#include "auth/common.hh"
#include "cql3/util.hh"
#include "seastar/coroutine/maybe_yield.hh"

namespace auth {

sstring role_info::describe(bool with_internals) const {
    sstring formatted_role_name = cql3::util::maybe_quote(role_name);

    const sstring role_part = role_name == meta::DEFAULT_SUPERUSER_NAME
            ? seastar::format("IF NOT EXISTS {}", formatted_role_name)
            : std::move(formatted_role_name);

    // `K_PASSWORD` in the grammar of CQL used by Scylla requires that passwords be quoted
    // with single quotation marks.
    const sstring salted_hash_part = maybe_salted_hash
            ? seastar::format(" SALTED HASH {} AND", cql3::util::single_quote(*maybe_salted_hash))
            : "";

    // fmt prints boolean values as `true` / `false` if the presentation type is not specified.
    return seastar::format("CREATE ROLE {} WITH{} LOGIN = {} AND SUPERUSER = {};",
            role_part, salted_hash_part, config.can_login, config.is_superuser);
}

future<std::vector<std::pair<sstring, sstring>>> role_to_directly_granted_map::describe_entities(bool with_internals) const {
    std::vector<std::pair<sstring, sstring>> result{};
    result.reserve(this->size());

    for (const auto& [granted_role, grantee_role] : *this) {
        const auto formatted_grantee = cql3::util::maybe_quote(grantee_role);
        const auto formatted_granted = cql3::util::maybe_quote(granted_role);
        result.emplace_back(granted_role, seastar::format("GRANT {} TO {};", formatted_granted, formatted_grantee));

        co_await coroutine::maybe_yield();
    }

    co_return result;
}

sstring attached_service_level::describe(bool with_internals) const {
    const auto formatted_role = cql3::util::maybe_quote(role_name);
    const auto formatted_sl = cql3::util::maybe_quote(service_level_name);
    return seastar::format("ATTACH SERVICE_LEVEL {} TO {};", formatted_sl, formatted_role);
}

} // namespace auth
