/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

// Seastar features.
#include <seastar/core/sstring.hh>

// STD.
#include <optional>
#include <unordered_set>
#include <map>

using namespace seastar;

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

} // namespace auth
