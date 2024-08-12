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

// STD.
#include <tuple>

namespace auth {

struct permission_details {
    sstring role_name;
    ::auth::resource resource;
    permission_set permissions;

    bool operator==(const permission_details& other) const {
        return std::forward_as_tuple(role_name, resource, permissions.mask())
                == std::forward_as_tuple(other.role_name, other.resource, other.permissions.mask());
    }

    auto operator<=>(const permission_details& other) const {
        return std::forward_as_tuple(role_name, resource, permissions)
                <=> std::forward_as_tuple(other.role_name, other.resource, other.permissions);
    }
};

} // namespace auth
