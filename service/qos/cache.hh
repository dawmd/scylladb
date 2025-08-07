/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include "service/qos/qos_common.hh"

#include <map>

using namespace seastar;

namespace auth {

class service;

} // namespace auth

namespace qos {

class service_level_controller;

class effective_service_level_controller {
private:
    [[maybe_unused]] service_level_controller& _sl_controller;
    [[maybe_unused]] auth::service& _auth_service;

    // Mappings `role name` -> `corresponding service level`.
    [[maybe_unused]] std::map<sstring, service_level_options> _mappings;

public:
    effective_service_level_controller(service_level_controller& sl_controller, auth::service& auth_service)
        : _sl_controller(sl_controller)
        , _auth_service(auth_service)
    {}

    future<std::optional<service_level_options>> find_effective_service_level([[maybe_unused]] const sstring& role_name) {
        // To be implemented in following commits.
        co_return std::nullopt;
    }
    std::optional<service_level_options> find_cached_effective_service_level([[maybe_unused]] const sstring& role_name) {
        // To be implemented in following commits.
        return std::nullopt;
    }

    service_level_controller& get_service_level_controller() noexcept {
        return _sl_controller;
    }
    const service_level_controller& get_service_level_controller() const noexcept {
        return _sl_controller;
    }

    future<> reload_cache() {
        // To be implemented in following commits.
        co_return;
    }
};

} // namespace qos
