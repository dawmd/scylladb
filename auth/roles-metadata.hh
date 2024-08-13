/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <optional>
#include <string_view>
#include <functional>

#include <seastar/core/future.hh>

#include "seastarx.hh"

namespace cql3 {
class query_processor;
class untyped_result_set_row;
}

namespace auth {

namespace meta::roles_table {

std::string_view creation_query();

constexpr std::string_view name = "roles";

constexpr std::string_view role_col_name = "role";
constexpr std::string_view can_login_col_name = "can_login";
constexpr std::string_view is_superuser_col_name = "is_superuser";
constexpr std::string_view member_of_col_name = "member_of";
constexpr std::string_view salted_hash_col_name = "salted_hash";

} // namespace meta::roles_table

///
/// Check that the default role satisfies a predicate, or `false` if the default role does not exist.
///
future<bool> default_role_row_satisfies(
        cql3::query_processor&,
        std::function<bool(const cql3::untyped_result_set_row&)>,
        std::optional<std::string> rolename = {}
        );

///
/// Check that any nondefault role satisfies a predicate. `false` if no nondefault roles exist.
///
future<bool> any_nondefault_role_row_satisfies(
        cql3::query_processor&,
        std::function<bool(const cql3::untyped_result_set_row&)>,
        std::optional<std::string> rolename = {}
        );

}
