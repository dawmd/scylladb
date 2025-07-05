/*
 * Copyright (C) 2025-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#include "db/view/view_options.hh"

#include "cql3/statements/cf_prop_defs.hh"

#include <ranges>

namespace db::view {

bool is_view_option(std::string_view option) {
    const auto cf_keywords = cql3::statements::cf_prop_defs::keywords();
    const auto cf_obsolete_keywords = cql3::statements::cf_prop_defs::obsolete_keywords();

    if (std::ranges::find(cf_keywords, option) != std::ranges::end(cf_keywords)) {
        return true;
    }
    if (std::ranges::find(cf_obsolete_keywords, option) != std::ranges::end(cf_obsolete_keywords)) {
        return true;
    }

    return false;
}

} // namespace db::view
