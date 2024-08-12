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
};

inline bool operator==(const permission_details& pd1, const permission_details& pd2) {
    return std::forward_as_tuple(pd1.role_name, pd1.resource, pd1.permissions.mask())
            == std::forward_as_tuple(pd2.role_name, pd2.resource, pd2.permissions.mask());
}

inline bool operator<(const permission_details& pd1, const permission_details& pd2) {
    return std::forward_as_tuple(pd1.role_name, pd1.resource, pd1.permissions)
            < std::forward_as_tuple(pd2.role_name, pd2.resource, pd2.permissions);
}

} // namespace auth
