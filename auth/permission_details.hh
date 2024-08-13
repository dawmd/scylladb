/*
 * Copyright (C) 2024-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

// Seastar features.
#include <seastar/core/sstring.hh>

// Scylla includes.
#include "auth/permission.hh"
#include "auth/resource.hh"
#include "data_dictionary/keyspace_element.hh"

// STD.
#include <tuple>
#include <utility>
#include <vector>

namespace replica {
class database;
} // namespace replica

namespace auth {

struct permission_details {
    sstring role_name;
    ::auth::resource resource;
    permission_set permissions;

    sstring keyspace_name() const {
        return "<null>";
    }

    sstring element_type([[maybe_unused]] const replica::database&) const {
        return "grant_permission";
    }

    std::vector<std::pair<sstring, sstring>> describe_elements(const replica::database&, bool with_internals) const;

    bool operator==(const permission_details& other) const {
        return std::forward_as_tuple(role_name, resource, permissions.mask())
                == std::forward_as_tuple(other.role_name, other.resource, other.permissions.mask());
    }

    auto operator<=>(const permission_details& other) const {
        return std::forward_as_tuple(role_name, resource, permissions)
                <=> std::forward_as_tuple(other.role_name, other.resource, other.permissions);
    }
};

static_assert(data_dictionary::keyspace_element_generator<permission_details>,
        "permission_details must satisfy the concept to be able to generate descriptions");

} // namespace auth
