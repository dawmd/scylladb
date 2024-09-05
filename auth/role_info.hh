/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

// Seastar features.
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

// Scylla includes.
#include "data_dictionary/describable_entity.hh"

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

struct role_info : public data_dictionary::describable_entity {
    sstring role_name;
    role_config config;
    std::optional<sstring> maybe_salted_hash;

    role_info() = default;
    role_info(const role_info&) = default;
    role_info& operator=(const role_info&) = default;
    role_info(role_info&&) = default;
    role_info& operator=(role_info&&) = default;

    role_info(sstring role_name_, role_config config_, std::optional<sstring> maybe_salted_hash_)
        : role_name(std::move(role_name_))
        , config(std::move(config_))
        , maybe_salted_hash(std::move(maybe_salted_hash_))
    {}

    virtual ~role_info() = default;

    virtual sstring entity_name() const override {
        return role_name;
    }

    virtual sstring entity_type() const override {
        return "role";
    }

    virtual sstring describe(bool with_internals) const override;
};

/// A key represents the role that has been granted another role.
/// A value represnts the role that has been granted TO another role.
struct role_to_directly_granted_map : public std::multimap<sstring, sstring>, data_dictionary::description_generator {
    role_to_directly_granted_map() = default;
    role_to_directly_granted_map(const role_to_directly_granted_map&) = default;
    role_to_directly_granted_map& operator=(const role_to_directly_granted_map&) = default;
    role_to_directly_granted_map(role_to_directly_granted_map&&) = default;
    role_to_directly_granted_map& operator=(role_to_directly_granted_map&&) = default;

    virtual ~role_to_directly_granted_map() = default;

    virtual sstring entity_type() const override {
        return "grant_role";
    }

    virtual future<std::vector<std::pair<sstring, sstring>>> describe_entities(bool with_internals) const override;
};

struct attached_service_level : public data_dictionary::describable_entity {
    sstring role_name;
    sstring service_level_name;

    attached_service_level() = default;
    attached_service_level(const attached_service_level&) = default;
    attached_service_level& operator=(const attached_service_level&) = default;
    attached_service_level(attached_service_level&&) = default;
    attached_service_level& operator=(attached_service_level&&) = default;

    attached_service_level(sstring role_name_, sstring service_level_name_)
        : role_name(std::move(role_name_))
        , service_level_name(std::move(service_level_name_))
    {}

    virtual ~attached_service_level() = default;

    virtual sstring entity_name() const override {
        return service_level_name;
    }

    virtual sstring entity_type() const override {
        return "service_level_attachment";
    }

    virtual sstring describe(bool with_internals) const override;
};

} // namespace auth
