/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include "cql3/statements/cf_properties.hh"
#include <seastar/core/sstring.hh>

#include <unordered_map>
#include <optional>

typedef std::unordered_map<sstring, sstring> index_options_map;

namespace cql3 {

namespace statements {

struct index_specific_prop_defs : public property_definitions {
    bool is_custom = false;
    std::optional<sstring> custom_class;

    void validate();
    index_options_map get_raw_options();
    index_options_map get_options();
};

class index_prop_defs : public cf_properties {
public:
    index_specific_prop_defs idx_opts;

    // Extract all of the index-specific options from this object and put them into `idx_opts`.
    void filter_options();
};

}
}

