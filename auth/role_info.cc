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
#include "cql3/util.hh"

namespace auth {

std::ostream& role_info::describe(std::ostream& os) const {
    // Note: fmt prints boolean values as `true` / `false` if the presentation type is not specified.

    const auto formatted_role_name = cql3::util::maybe_quote(role_name);

    if (maybe_salted_hash) {
        // Note: Although `crypt` functions don't produce whitespace characters, etc., they don't guarantee
        //       that the salted hash won't contain a quotation mark:
        //
        //         "Hashed passphrases are always entirely printable ASCII, and do not contain any whitespace
        //          or the characters `:`, `;`, `*`, `!`, or `\`."
        //
        //       --- crypt(5); I changed the formatting of the manual page a bit.
        //
        //       Because of that, we call `cql3::util::maybe_quote` for safety.
        const auto salted_hash = cql3::util::maybe_quote(*maybe_salted_hash);
        fmt::print(os, "CREATE ROLE IF NOT EXISTS {} WITH SALTED_HASH = {} AND LOGIN = {} AND SUPERUSER = {};",
                formatted_role_name, salted_hash, config.can_login, config.is_superuser);
    } else {
        fmt::print(os, "CREATE ROLE IF NOT EXISTS {} WITH LOGIN = {} AND SUPERUSER = {};",
                formatted_role_name, config.can_login, config.is_superuser);
    }

    return os;
}

std::vector<std::pair<sstring, sstring>> role_grants::describe_elements(const replica::database&, bool with_internals) const {
    std::vector<std::pair<sstring, sstring>> result{};
    result.reserve(granted_roles.size());

    const auto formatted_grantee = cql3::util::maybe_quote(grantee_role);

    for (const auto& granted_role : granted_roles) {
        const auto formatted_granted = cql3::util::maybe_quote(granted_role);
        result.emplace_back(formatted_grantee, seastar::format("GRANT {} to {};", formatted_granted, formatted_grantee));
    }

    return result;
}

std::ostream& attached_service_levels::describe(std::ostream& os) const {
    const auto formatted_role = cql3::util::maybe_quote(role_name);
    const auto formatted_sl = cql3::util::maybe_quote(service_level_name);
    fmt::print(os, "ATTACH SERVICE LEVEL {} to {};", formatted_role, formatted_sl);
    return os;
}

} // namespace auth
