/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#include <set>
#include <seastar/core/format.hh>
#include "index_prop_defs.hh"
#include "cql3/statements/property_definitions.hh"
#include "index/secondary_index.hh"
#include "exceptions/exceptions.hh"

#include <variant>

namespace cql3::statements {

namespace {

const sstring KW_OPTIONS = "options";

} // anonymous namespace

void index_specific_prop_defs::validate() {
    static std::set<sstring> keywords{KW_OPTIONS};

    property_definitions::validate(keywords);

    if (is_custom && !custom_class) {
        throw exceptions::invalid_request_exception("CUSTOM index requires specifying the index class");
    }
    
    if (!custom_class && !_properties.empty()) {
        throw exceptions::invalid_request_exception("Cannot specify options for a non-CUSTOM index");
    }
    if (get_raw_options().count(
            db::index::secondary_index::custom_index_option_name)) {
        throw exceptions::invalid_request_exception(
                format("Cannot specify {} as a CUSTOM option",
                        db::index::secondary_index::custom_index_option_name));
    }

}

index_options_map
index_specific_prop_defs::get_raw_options() {
    auto options = get_map(KW_OPTIONS);
    return !options ? std::unordered_map<sstring, sstring>() : std::unordered_map<sstring, sstring>(options->begin(), options->end());
}

index_options_map
index_specific_prop_defs::get_options() {
    auto options = get_raw_options();
    options.emplace(db::index::secondary_index::custom_index_option_name, *custom_class);
    return options;
}

void index_prop_defs::filter_options() {
    if (_properties->has_property(KW_OPTIONS)) {
        auto extracted_opt = _properties->remove_property(KW_OPTIONS);
        if (std::holds_alternative<sstring>(extracted_opt)) {
            idx_opts.add_property(KW_OPTIONS, std::move(std::get<sstring>(extracted_opt)));
        } else if (std::holds_alternative<typename property_definitions::map_type>(extracted_opt)) {
            idx_opts.add_property(KW_OPTIONS, std::move(std::get<typename property_definitions::map_type>(extracted_opt)));
        }
    }
}

} // namespace cql3::statements
