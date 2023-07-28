/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/sstring.hh>

#include "locator/host_id.hh"
#include "locator/topology.hh"

#include <iostream>
#include <stdexcept>
#include <string_view>
#include <unordered_set>

namespace db::hints {

class host_filter final {
private:
    enum class enabled_kind {
        enabled_for_all,
        enabled_selectively,
        disabled_for_all
    };

public:
    struct enabled_for_all_tag {};
    struct disabled_for_all_tag {};

private:
    enabled_kind _enabled_kind;
    std::unordered_set<seastar::sstring> _dcs;

public:
    host_filter(enabled_for_all_tag = {});
    host_filter(disabled_for_all_tag);
    explicit host_filter(std::unordered_set<seastar::sstring> allowed_dcs);
    
    static host_filter parse_from_config_string(seastar::sstring opt);
    static host_filter parse_from_dc_list(seastar::sstring opt);

    bool can_hint_for(const locator::topology& topo, locator::host_id host_id) const;
    bool is_enabled_for_all() const noexcept {
        return _enabled_kind == enabled_kind::enabled_for_all;
    }
    bool is_disabled_for_all() const noexcept {
        return _enabled_kind == enabled_kind::disabled_for_all;
    }

    seastar::sstring to_configuration_string() const;
    
    const std::unordered_set<seastar::sstring>& get_dcs() const {
        return _dcs;
    }

    bool operator==(const host_filter& other) const noexcept = default;
    friend std::ostream& operator<<(std::ostream& os, const host_filter& f);

private:
    static std::string_view enabled_kind_to_string_view(enabled_kind);
};

std::istream& operator>>(std::istream& is, host_filter& f);

class hints_configuration_parse_error : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

} // namespace db::hints
