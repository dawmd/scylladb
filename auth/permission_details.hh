/*
 * Copyright (C) 2024-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <seastar/core/sstring.hh>

#include "auth/permission.hh"
#include "auth/resource.hh"
#include "data_dictionary/describable_entity.hh"

#include <tuple>
#include <utility>
#include <vector>

namespace replica {
class database;
} // namespace replica

namespace auth {

struct permission_details : public data_dictionary::description_generator {
    sstring role_name;
    ::auth::resource resource;
    permission_set permissions;

    permission_details(sstring role_name_, auth::resource resuorce_, permission_set permissions_)
        : role_name(std::move(role_name_))
        , resource(std::move(resuorce_))
        , permissions(std::move(permissions_))
    {}

    permission_details(const permission_details&) = default;
    permission_details& operator=(const permission_details&) = default;
    permission_details(permission_details&&) = default;
    permission_details& operator=(permission_details&&) = default;

    virtual ~permission_details() = default;

    bool operator==(const permission_details& other) const {
        return std::forward_as_tuple(role_name, resource, permissions.mask())
                == std::forward_as_tuple(other.role_name, other.resource, other.permissions.mask());
    }

    auto operator<=>(const permission_details& other) const {
        return std::forward_as_tuple(role_name, resource, permissions)
                <=> std::forward_as_tuple(other.role_name, other.resource, other.permissions);
    }

    virtual sstring entity_type() const override {
        return "grant_permission";
    }

    virtual future<std::vector<std::pair<sstring, sstring>>> describe_entities(bool with_internals) const override;
};

} // namespace auth
