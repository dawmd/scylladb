/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

// Seastar features.
#include <seastar/core/sstring.hh>

// Scylla includes.
#include "data_dictionary/keyspace_element.hh"

// STD.
#include <optional>
#include <unordered_set>
#include <map>
#include <utility>
#include <vector>

using namespace seastar;

namespace replica {
class database;
} // namespace replica

namespace auth {

using role_set = std::unordered_set<sstring>;
using role_to_directly_granted_map = std::multimap<sstring, sstring>;

struct role_config final {
    bool is_superuser{false};
    bool can_login{false};
};

///
/// Differential update for altering existing roles.
///
struct role_config_update final {
    std::optional<bool> is_superuser{};
    std::optional<bool> can_login{};
};

struct role_info : public data_dictionary::keyspace_element {
    sstring role_name;
    role_config config;
    std::optional<sstring> maybe_salted_hash;

    virtual ~role_info() = default;

    virtual sstring keyspace_name() const override {
        return "<null>";
    }

    virtual sstring element_name() const override {
        return role_name;
    }

    virtual sstring element_type() const override {
        return "role";
    }

    virtual std::ostream& describe(std::ostream& os) const override;
};

struct role_grants {
    sstring grantee_role;
    std::vector<sstring> granted_roles;

    sstring keyspace_name() const {
        return "<null>";
    }

    sstring element_type([[maybe_unused]] const replica::database&) const {
        return "grant_role";
    }

    std::vector<std::pair<sstring, sstring>> describe_elements(const replica::database&, bool with_internals) const;
};

static_assert(data_dictionary::keyspace_element_generator<role_grants>,
        "role_grants must satisfy the concept to be able to generate descriptions");

struct attached_service_levels : public data_dictionary::keyspace_element {
    sstring role_name;
    sstring service_level_name;

    virtual ~attached_service_levels() = default;

    virtual sstring keyspace_name() const override {
        return "<null>";
    }

    virtual sstring element_name() const override {
        return role_name;
    }

    virtual sstring element_type() const override {
        return "service_level_attachment";
    }

    virtual std::ostream& describe(std::ostream& os) const override;
};

} // namespace auth
