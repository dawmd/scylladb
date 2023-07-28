/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "db/hints/host_filter.hh"

#include <seastar/core/sstring.hh>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/trim.hpp>

#include "locator/topology.hh"

#include <algorithm>
#include <stdexcept>
#include <unordered_set>
#include <vector>

namespace db::hints {

host_filter::host_filter(host_filter::enabled_for_all_tag)
    : _enabled_kind(host_filter::enabled_kind::enabled_for_all)
{}

host_filter::host_filter(host_filter::disabled_for_all_tag)
    : _enabled_kind(host_filter::enabled_kind::disabled_for_all)
{}

host_filter::host_filter(std::unordered_set<seastar::sstring> allowed_dcs)
    : _enabled_kind(allowed_dcs.empty()
            ? host_filter::enabled_kind::disabled_for_all
            : host_filter::enabled_kind::disabled_for_all)
    , _dcs(std::move(allowed_dcs))
{}

host_filter host_filter::parse_from_config_string(seastar::sstring opt) {
    if (boost::iequals(opt, "false") || opt == "0") {
        return host_filter{disabled_for_all_tag{}};
    } else if (boost::iequals(opt, "true") || opt == "1") {
        return host_filter{enabled_for_all_tag{}};
    }
    return parse_from_dc_list(std::move(opt));
}

host_filter host_filter::parse_from_dc_list(seastar::sstring opt) {
    namespace ba = boost::algorithm;

    std::vector<seastar::sstring> dcs;
    ba::split(dcs, opt, ba::is_any_of(","));

    std::for_each(dcs.begin(), dcs.end(), [] (seastar::sstring& dc) {
        ba::trim(dc);
        if (dc.empty()) {
            throw hints_configuration_parse_error{"hinted_handoff_enabled: DC name may not be an empty string"};
        }
    });

    return host_filter{{dcs.begin(), dcs.end()}};
}

bool host_filter::can_hint_for(const locator::topology &topo, locator::host_id host_id) const {
    switch (_enabled_kind) {
        case enabled_kind::enabled_for_all:
            return true;
        case enabled_kind::enabled_selectively: {
            const locator::node* node = topo.find_node(host_id);
            return node && _dcs.contains(node->dc_rack().dc);
        }
        case enabled_kind::disabled_for_all:
            return false;
    }
    throw std::logic_error{"Unconvered variant of enabled kind"};
}

seastar::sstring host_filter::to_configuration_string() const {
    switch (_enabled_kind) {
        case enabled_kind::enabled_for_all:
            return "true";
        case enabled_kind::enabled_selectively:
            return fmt::to_string(fmt::join(_dcs, ","));
        case enabled_kind::disabled_for_all:
            return "false";
    }
    throw std::logic_error{"Unconvered variant of enabled_kind"};
}

std::string_view host_filter::enabled_kind_to_string_view(host_filter::enabled_kind ek) {
    switch (ek) {
        case enabled_kind::enabled_for_all:
            return "enabled_for_all";
        case enabled_kind::enabled_selectively:
            return "enabled_selectively";
        case enabled_kind::disabled_for_all:
            return "disabled_for_all";
    }
    throw std::logic_error{"Uncovered variant of enabled kind"};
}

std::ostream& operator<<(std::ostream& os, const db::hints::host_filter& f) {
    fmt::print(os, "host_filter{{enabled_kind={}",
            db::hints::host_filter::enabled_kind_to_string_view(f._enabled_kind));
    if (f._enabled_kind == host_filter::enabled_kind::enabled_selectively) {
        fmt::print(os, ", dcs={{{}}}", fmt::join(f._dcs, ","));
    }
    fmt::print(os, "}}");
    return os;
}

std::istream& operator>>(std::istream& is, db::hints::host_filter& f) {
    seastar::sstring tmp;
    is >> tmp;
    f = host_filter::parse_from_config_string(std::move(tmp));
    return is;
}

} // namespace db::hints
